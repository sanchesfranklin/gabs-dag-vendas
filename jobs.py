"""Etapas do pipeline de vendas: carga inicial completa, depois incremental.

Toda etapa recebe uma JANELA de datas [inicio, fim) e só toca essa fatia:

    landing  grava em  s3a://landing/vendas/pedidos/dt_referencia=AAAA-MM-DD/
    bronze   substitui DT_REFERENCIA dentro da janela
    silver   substitui DT_REFERENCIA dentro da janela
    gold     substitui DT_PEDIDO     dentro da janela

A janela é decidida uma vez só, na landing:

    execução diária   [dia, dia + 1)          só os pedidos do dia
    carga completa    [sem início, fim)       todo o histórico até o fim

A carga é completa quando o produto ainda não foi carregado até a gold, ou
quando a DAG é disparada com o parâmetro carga_completa. Nos dois casos os
dados continuam particionados por dia; a diferença é quantos dias entram.

Rodar a mesma janela de novo substitui exatamente a mesma fatia, com os mesmos
dados de origem. É isso que torna o pipeline idempotente.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gabs import storage, tables
from gabs.spark import get_spark_session
from vendas import pipeline as p
from vendas.schema_tables import PEDIDOS, SCHEMA_ORIGEM_PEDIDOS

# Nome da partição na landing (em minúsculas, como as colunas da origem)
PARTICAO_LANDING = "dt_referencia"


def _caminho_landing() -> str:
    """Pasta da tabela na landing. Cada dia fica numa subpasta dt_referencia=AAAA-MM-DD."""
    return storage.path("landing", p.PRODUTO, p.TABELA_PEDIDOS)


def _filtro(coluna: str, inicio: str | None, fim: str) -> str:
    """Condição SQL da janela [inicio, fim). Sem início, vale todo o histórico até o fim.

    >>> _filtro("DT_REFERENCIA", "2026-09-10", "2026-09-11")
    "DT_REFERENCIA >= '2026-09-10' AND DT_REFERENCIA < '2026-09-11'"
    >>> _filtro("DT_REFERENCIA", None, "2026-10-18")
    "DT_REFERENCIA < '2026-10-18'"
    """
    condicao = f"{coluna} < '{fim}'"
    if inicio:
        condicao = f"{coluna} >= '{inicio}' AND {condicao}"
    return condicao


def _consulta_origem(inicio: str | None, fim: str) -> str:
    """SQL que a origem executa: só os pedidos criados dentro da janela."""
    return (
        f"(SELECT * FROM {p.TABELA_ORIGEM} "
        f"WHERE {_filtro(p.COLUNA_DATA_ORIGEM, inicio, fim)}) AS pedidos_da_janela"
    )


def _produto_carregado(spark: SparkSession) -> bool:
    """A carga inicial já foi até o fim?

    Olha a última camada: a gold só existe depois que uma execução passou por
    todas as etapas. Se a carga inicial falhar no meio (na silver, por
    exemplo), a próxima execução faz a carga completa de novo, em vez de
    seguir só com o dia e deixar o histórico pela metade.
    """
    return DeltaTable.isDeltaTable(
        spark, storage.path("gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO)
    )


def extrair_landing(inicio: str, fim: str, carga_completa: bool = False) -> dict:
    """Origem -> landing: os pedidos da janela, sem alterar nada, em JSON.

    Decide a janela da execução e a devolve para as próximas etapas:
        {"inicio": "2026-10-17" ou None, "fim": "2026-10-18", "quantidade": 12}
    """
    spark = get_spark_session(f"{p.PRODUTO}_landing")
    try:
        if carga_completa or not _produto_carregado(spark):
            inicio = None
            print(f"Carga completa: todos os pedidos criados antes de {fim}.")
        else:
            print(f"Carga incremental: pedidos de {inicio} até antes de {fim}.")

        df = (
            spark.read.format("jdbc")
            .option("url", os.environ["FONTE_JDBC_URL"])
            # Em vez do nome da tabela, uma subconsulta: o filtro roda no
            # PostgreSQL, e só os pedidos da janela trafegam pela rede.
            .option("dbtable", _consulta_origem(inicio, fim))
            .option("user", os.environ["FONTE_USER"])
            .option("password", os.environ["FONTE_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
            # O dia de cada pedido vira a pasta onde ele é gravado. Com
            # partitionBy, a coluna sai do conteúdo do arquivo e fica só no
            # nome da pasta: o JSON continua igual ao da origem.
            .withColumn(PARTICAO_LANDING, F.to_date(p.COLUNA_DATA_ORIGEM))
        )
        df.cache()  # a origem é lida uma vez só, para contar e para gravar
        quantidade = df.count()

        (
            df.write.mode("overwrite")
            .partitionBy(PARTICAO_LANDING)
            # "dynamic": substitui só as pastas dos dias que vieram nesta
            # gravação. Sem isso, o overwrite apagaria a landing inteira.
            .option("partitionOverwriteMode", "dynamic")
            .json(_caminho_landing())
        )
        print(f"Pedidos extraídos: {quantidade}")
        return {"inicio": inicio, "fim": fim, "quantidade": quantidade}
    finally:
        spark.stop()


def ler_landing(spark: SparkSession, inicio: str | None, fim: str) -> DataFrame:
    """Lê da landing os dias da janela, já com a coluna DT_REFERENCIA.

    O Spark lê a pasta da tabela e descobre os dias pelos nomes das
    subpastas. O filtro na partição faz ele abrir só as pastas da janela.

    A partição vira DT_REFERENCIA num select, e não com withColumn + drop:
    o Spark não diferencia maiúsculas de minúsculas nos nomes de coluna, e
    drop("dt_referencia") apagaria também a DT_REFERENCIA.
    """
    return (
        spark.read.schema(SCHEMA_ORIGEM_PEDIDOS).json(_caminho_landing())
        .filter(_filtro(PARTICAO_LANDING, inicio, fim))
        .select(
            *SCHEMA_ORIGEM_PEDIDOS.fieldNames(),
            F.col(PARTICAO_LANDING).cast("date").alias(p.COLUNA_REFERENCIA),
        )
    )


def carregar_bronze(inicio: str | None, fim: str) -> None:
    """Landing -> bronze: os mesmos dados, em Delta, com o dia de referência."""
    spark = get_spark_session(f"{p.PRODUTO}_bronze")
    try:
        df = ler_landing(spark, inicio, fim).withColumn(
            "DH_PROCESSAMENTO", F.current_timestamp()
        )
        tables.write_table(
            df, "bronze", p.PRODUTO, p.TABELA_PEDIDOS,
            replace_where=_filtro(p.COLUNA_REFERENCIA, inicio, fim),
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


def tratar_silver(inicio: str | None, fim: str) -> None:
    """Bronze -> silver: renomeia, tipa, padroniza textos e remove duplicatas."""
    spark = get_spark_session(f"{p.PRODUTO}_silver")
    try:
        bronze = tables.read_table(spark, "bronze", p.PRODUTO, p.TABELA_PEDIDOS).filter(
            _filtro(p.COLUNA_REFERENCIA, inicio, fim)
        )
        tables.write_table(
            padronizar_pedidos(bronze), "silver", p.PRODUTO, p.TABELA_PEDIDOS,
            replace_where=_filtro(p.COLUNA_REFERENCIA, inicio, fim),
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


def gerar_gold(inicio: str | None, fim: str) -> None:
    """Silver -> gold: resumo por dia, pronto para o BI."""
    spark = get_spark_session(f"{p.PRODUTO}_gold")
    try:
        silver = tables.read_table(spark, "silver", p.PRODUTO, p.TABELA_PEDIDOS).filter(
            _filtro(p.COLUNA_REFERENCIA, inicio, fim)
        )
        tables.write_table(
            resumir_por_dia(silver), "gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO,
            replace_where=_filtro("DT_PEDIDO", inicio, fim),
            partition_by=["DT_PEDIDO"],
        )
    finally:
        spark.stop()


def resetar_produto() -> None:
    """Apaga as tabelas e os arquivos do produto em todas as camadas.

    Uso pontual, para recomeçar do zero (por exemplo, quando a estrutura das
    tabelas muda). A execução seguinte faz a carga completa. Não existe desfazer.
    """
    spark = get_spark_session(f"{p.PRODUTO}_reset")
    try:
        tables.drop_table(spark, "bronze", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.drop_table(spark, "silver", p.PRODUTO, p.TABELA_PEDIDOS)
        tables.drop_table(spark, "gold", p.PRODUTO, p.TABELA_RESUMO_DIARIO)
        tables.delete_path(spark, _caminho_landing())
    finally:
        spark.stop()
