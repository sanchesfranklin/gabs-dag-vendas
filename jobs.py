"""Etapas do pipeline de vendas, em modo incremental.

Cada execução processa UM dia de referência (os pedidos criados naquele dia).
Todas as etapas recebem esse dia e só tocam a fatia correspondente:

    landing  grava em  s3a://landing/vendas/pedidos/dt_referencia=AAAA-MM-DD/
    bronze   substitui DT_REFERENCIA = dia
    silver   substitui DT_REFERENCIA = dia
    gold     substitui DT_PEDIDO     = dia

Rodar o mesmo dia de novo substitui exatamente a mesma fatia, com os mesmos
dados de origem. É isso que torna o pipeline idempotente.
"""

import os
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gabs import storage, tables
from gabs.spark import get_spark_session
from vendas import pipeline as p
from vendas.schema_tables import PEDIDOS, SCHEMA_ORIGEM_PEDIDOS


def _caminho_landing(dia: str) -> str:
    """Pasta do dia na landing: s3a://landing/vendas/pedidos/dt_referencia=AAAA-MM-DD"""
    return f"{storage.path('landing', p.PRODUTO, p.TABELA_PEDIDOS)}/dt_referencia={dia}"


def _consulta_do_dia(dia: str) -> str:
    """SQL que a origem executa: só os pedidos criados no dia [dia, dia + 1)."""
    inicio = date.fromisoformat(dia)
    fim = inicio + timedelta(days=1)
    return (
        f"(SELECT * FROM {p.TABELA_ORIGEM} "
        f"WHERE {p.COLUNA_DATA_ORIGEM} >= '{inicio.isoformat()}' "
        f"AND {p.COLUNA_DATA_ORIGEM} < '{fim.isoformat()}') AS pedidos_do_dia"
    )


def extrair_landing(dia: str) -> int:
    """Origem -> landing: os pedidos do dia, sem alterar nada, em JSON.

    Devolve quantos pedidos vieram, para a DAG decidir se segue adiante.
    Mesmo com zero pedidos a pasta do dia é gravada (vazia): fica registrado
    que a coleta aconteceu e não encontrou nada.
    """
    spark = get_spark_session(f"{p.PRODUTO}_landing")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", os.environ["FONTE_JDBC_URL"])
            # Em vez do nome da tabela, uma subconsulta: o filtro roda no
            # PostgreSQL, e só os pedidos do dia trafegam pela rede.
            .option("dbtable", _consulta_do_dia(dia))
            .option("user", os.environ["FONTE_USER"])
            .option("password", os.environ["FONTE_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        df.cache()  # a origem é lida uma vez só, para contar e para gravar
        quantidade = df.count()
        df.write.mode("overwrite").json(_caminho_landing(dia))
        return quantidade
    finally:
        spark.stop()


def carregar_bronze(dia: str) -> None:
    """Landing -> bronze: os mesmos dados, em Delta, com o dia de referência."""
    spark = get_spark_session(f"{p.PRODUTO}_bronze")
    try:
        df = (
            spark.read.schema(SCHEMA_ORIGEM_PEDIDOS).json(_caminho_landing(dia))
            .withColumn(p.COLUNA_REFERENCIA, F.lit(dia).cast("date"))
            .withColumn("DH_PROCESSAMENTO", F.current_timestamp())
        )
        tables.write_table(
            df, "bronze", p.PRODUTO, p.TABELA_PEDIDOS,
            replace_where=f"{p.COLUNA_REFERENCIA} = '{dia}'",
            partition_by=[p.COLUNA_REFERENCIA],
        )
    finally:
        spark.stop()


def padronizar_pedidos(df: DataFrame) -> DataFrame:
    """Regras da silver, separadas da leitura e da gravação para poderem ser testadas."""
    colunas = [
        F.col(origem).cast(tipo).alias(destino)
        for origem, (destino, tipo) in PEDIDOS.items()
    ]
    return (
        df.select(*colunas, p.COLUNA_REFERENCIA)
        .withColumn("SG_UF", F.upper(F.trim("SG_UF")))
        .withColumn("DS_STATUS", F.lower(F.trim("DS_STATUS")))
        .dropDuplicates(["ID_PEDIDO"])
    )


def tratar_silver(dia: str) -> None:
    """Bronze -> silver: renomeia, tipa, padroniza textos e remove duplicatas do dia."""
    spark = get_spark_session(f"{p.PRODUTO}_silver")
    try:
        bronze = (
            tables.read_table(spark, "bronze", p.PRODUTO, p.TABELA_PEDIDOS)
            .filter(F.col(p.COLUNA_REFERENCIA) == F.lit(dia).cast("date"))
        )
        tables.write_table(
            padronizar_pedidos(bronze), "silver", p.PRODUTO, p.TABELA_PEDIDOS,
            replace_where=f"{p.COLUNA_REFERENCIA} = '{dia}'",
            partition_by=[p.COLUNA_REFERENCIA],
        )
    finally:
        spark.stop()


def resumir_por_dia(df: DataFrame) -> DataFrame:
    """Regras da gold: indicadores diários por UF, só de pedidos pagos."""
    return (
        df.filter(F.col("DS_STATUS") == p.STATUS_PAGO)
        .groupBy(F.to_date("DH_CRIACAO").alias("DT_PEDIDO"), "SG_UF")
        .agg(
            F.count("*").alias("QTD_PEDIDOS"),
            F.sum("VL_PEDIDO").alias("VL_TOTAL"),
        )
        .withColumn("VL_TICKET_MEDIO", F.round(F.col("VL_TOTAL") / F.col("QTD_PEDIDOS"), 2))
    )


def gerar_gold(dia: str) -> None:
    """Silver -> gold: resumo do dia, pronto para o BI."""
    spark = get_spark_session(f"{p.PRODUTO}_gold")
    try:
        silver = (
            tables.read_table(spark, "silver", p.PRODUTO, p.TABELA_PEDIDOS)
            .filter(F.col(p.COLUNA_REFERENCIA) == F.lit(dia).cast("date"))
        )
        tables.write_table(
            resumir_por_dia(silver), "gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO,
            replace_where=f"DT_PEDIDO = '{dia}'",
            partition_by=["DT_PEDIDO"],
        )
    finally:
        spark.stop()


def resetar_produto() -> None:
    """Apaga as tabelas e os arquivos do produto em todas as camadas.

    Uso único, ao migrar da versão de carga completa para a incremental, já
    que a estrutura das tabelas mudou. Não existe desfazer.
    """
    spark = get_spark_session(f"{p.PRODUTO}_reset")
    try:
        tables.drop_table(spark, "bronze", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.drop_table(spark, "silver", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.drop_table(spark, "gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO)
        tables.delete_path(spark, storage.path("landing", p.PRODUTO, p.TABELA_PEDIDOS))
    finally:
        spark.stop()
