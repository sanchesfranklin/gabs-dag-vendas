"""Etapas do pipeline de vendas.

Cada função é uma etapa independente: abre a própria sessão Spark, lê da
camada anterior, grava na seguinte e fecha a sessão. Rodar a mesma etapa duas
vezes produz o mesmo resultado (idempotência), o que permite repetir uma
tarefa que falhou sem medo.
"""

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gabs import storage, tables
from gabs.spark import get_spark_session
from vendas import pipeline as p
from vendas.schema_tables import PEDIDOS


def _caminho_landing(data_carga: str) -> str:
    """Pasta da carga do dia na landing: s3a://landing/vendas/pedidos/dt_carga=AAAA-MM-DD"""
    return f"{storage.path('landing', p.PRODUTO, p.TABELA_PEDIDOS)}/dt_carga={data_carga}"


def extrair_landing(data_carga: str) -> None:
    """Origem -> landing: copia a tabela do PostgreSQL, sem alterar nada, em JSON."""
    spark = get_spark_session(f"{p.PRODUTO}_landing")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", os.environ["FONTE_JDBC_URL"])
            .option("dbtable", p.TABELA_ORIGEM)
            .option("user", os.environ["FONTE_USER"])
            .option("password", os.environ["FONTE_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )
        df.write.mode("overwrite").json(_caminho_landing(data_carga))
    finally:
        spark.stop()


def carregar_bronze(data_carga: str) -> None:
    """Landing -> bronze: mesmos dados, agora em tabela Delta, com data da carga."""
    spark = get_spark_session(f"{p.PRODUTO}_bronze")
    try:
        df = (
            spark.read.json(_caminho_landing(data_carga))
            .withColumn("DT_CARGA", F.lit(data_carga).cast("date"))
            .withColumn("DH_PROCESSAMENTO", F.current_timestamp())
        )
        tables.write_table(df, "bronze", p.PRODUTO, p.TABELA_PEDIDOS)
    finally:
        spark.stop()


def padronizar_pedidos(df: DataFrame) -> DataFrame:
    """Regras da silver, separadas da leitura e da gravação para poderem ser testadas."""
    colunas = [
        F.col(origem).cast(tipo).alias(destino)
        for origem, (destino, tipo) in PEDIDOS.items()
    ]
    return (
        df.select(*colunas)
        .withColumn("SG_UF", F.upper(F.trim("SG_UF")))
        .withColumn("DS_STATUS", F.lower(F.trim("DS_STATUS")))
        .dropDuplicates(["ID_PEDIDO"])
    )


def tratar_silver() -> None:
    """Bronze -> silver: renomeia, tipa, padroniza textos e remove duplicatas."""
    spark = get_spark_session(f"{p.PRODUTO}_silver")
    try:
        bronze = tables.read_table(spark, "bronze", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.write_table(padronizar_pedidos(bronze), "silver", p.PRODUTO, p.TABELA_PEDIDOS)
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


def gerar_gold() -> None:
    """Silver -> gold: tabela pronta para o BI."""
    spark = get_spark_session(f"{p.PRODUTO}_gold")
    try:
        silver = tables.read_table(spark, "silver", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.write_table(resumir_por_dia(silver), "gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO)
    finally:
        spark.stop()
