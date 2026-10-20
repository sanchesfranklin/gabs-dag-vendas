"""Etapa gold: silver -> gold, o resumo diário de vendas por UF."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from gabs import tables
from gabs.spark import get_spark_session
from vendas import config
from vendas.janela import Janela


def resumir_por_dia(df: DataFrame) -> DataFrame:
    """Regras da gold: quantidade, valor total e ticket médio por dia e UF, só de pedidos pagos.

    Separada da leitura e da gravação para poder ser testada com dados em memória.
    """
    return (
        df.filter(F.col("DS_STATUS") == config.STATUS_PAGO)
        .groupBy(F.to_date("DH_CRIACAO").alias(config.COLUNA_DATA_PEDIDO), "SG_UF")
        .agg(
            F.count("*").alias("QTD_PEDIDOS"),
            F.sum("VL_PEDIDO").alias("VL_TOTAL"),
        )
        .withColumn("VL_TICKET_MEDIO", F.round(F.col("VL_TOTAL") / F.col("QTD_PEDIDOS"), 2))
    )


def executar(janela: Janela) -> None:
    """Substitui na gold os dias da janela pelo resumo dos pedidos da silver."""
    spark = get_spark_session(f"{config.PRODUTO}_gold")
    try:
        silver = tables.read_table(
            spark, "silver", config.PRODUTO, config.TABELA_PEDIDOS
        ).filter(janela.filtro(config.COLUNA_REFERENCIA))
        tables.write_table(
            resumir_por_dia(silver), "gold", config.PRODUTO, config.TABELA_RESUMO_DIARIO,
            replace_where=janela.filtro(config.COLUNA_DATA_PEDIDO),
            partition_by=[config.COLUNA_DATA_PEDIDO],
        )
    finally:
        spark.stop()
