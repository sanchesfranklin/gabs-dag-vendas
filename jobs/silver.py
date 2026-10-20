"""Etapa silver: bronze -> silver, pedidos padronizados e sem duplicatas."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gabs import tables
from gabs.spark import get_spark_session
from vendas import config
from vendas.janela import Janela
from vendas.schemas import PEDIDOS


def padronizar_pedidos(df: DataFrame) -> DataFrame:
    """Regras da silver: renomeia, tipa, padroniza textos e remove duplicatas.

    Separada da leitura e da gravação para poder ser testada com dados em memória.
    """
    colunas = [
        F.col(origem).cast(tipo).alias(destino)
        for origem, (destino, tipo) in PEDIDOS.items()
    ]
    return (
        df.select(*colunas, config.COLUNA_REFERENCIA)
        .withColumn("SG_UF", F.upper(F.trim("SG_UF")))
        .withColumn("DS_STATUS", F.lower(F.trim("DS_STATUS")))
        .dropDuplicates(["ID_PEDIDO"])
    )


def executar(janela: Janela) -> None:
    """Substitui na silver os dias da janela pelos pedidos padronizados da bronze."""
    spark = get_spark_session(f"{config.PRODUTO}_silver")
    try:
        bronze = tables.read_table(
            spark, "bronze", config.PRODUTO, config.TABELA_PEDIDOS
        ).filter(janela.filtro(config.COLUNA_REFERENCIA))
        tables.write_table(
            padronizar_pedidos(bronze), "silver", config.PRODUTO, config.TABELA_PEDIDOS,
            replace_where=janela.filtro(config.COLUNA_REFERENCIA),
            partition_by=[config.COLUNA_REFERENCIA],
        )
    finally:
        spark.stop()
