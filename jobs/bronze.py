"""Etapa bronze: landing -> bronze, os mesmos dados em Delta, com o dia de referência."""

from pyspark.sql import functions as F

from gabs import tables
from gabs.spark import get_spark_session
from vendas import config
from vendas.conectores import landing
from vendas.janela import Janela


def executar(janela: Janela) -> None:
    """Substitui na bronze os dias da janela pelo conteúdo da landing."""
    spark = get_spark_session(f"{config.PRODUTO}_bronze")
    try:
        pedidos = landing.ler_pedidos(spark, janela).withColumn(
            "DH_PROCESSAMENTO", F.current_timestamp()
        )
        tables.write_table(
            pedidos, "bronze", config.PRODUTO, config.TABELA_PEDIDOS,
            replace_where=janela.filtro(config.COLUNA_REFERENCIA),
            partition_by=[config.COLUNA_REFERENCIA],
        )
    finally:
        spark.stop()
