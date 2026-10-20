"""Etapa landing: origem -> landing, os pedidos da janela em JSON, sem alteração.

É aqui que a janela EFETIVA da execução é decidida, e devolvida para as
etapas seguintes. A carga é completa quando a DAG pediu, ou quando o produto
ainda não foi carregado até a gold.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from gabs import storage
from gabs.spark import get_spark_session
from vendas import config
from vendas.conectores import landing, origem
from vendas.config import ConexaoOrigem
from vendas.janela import Janela

log = logging.getLogger(__name__)


def executar(janela: Janela) -> tuple[Janela, int]:
    """Extrai os pedidos da janela e devolve a janela efetiva e a quantidade extraída."""
    spark = get_spark_session(f"{config.PRODUTO}_landing")
    try:
        janela = resolver_janela(spark, janela)
        tipo = "completa" if janela.completa else "incremental"
        log.info("Carga %s: pedidos da janela %s.", tipo, janela)

        pedidos = origem.ler_pedidos(spark, janela, ConexaoOrigem.do_ambiente())
        pedidos.cache()  # a origem é lida uma vez só, para contar e para gravar
        quantidade = pedidos.count()
        landing.gravar_pedidos(pedidos)

        log.info("Pedidos extraídos: %d", quantidade)
        return janela, quantidade
    finally:
        spark.stop()


def produto_carregado(spark: SparkSession) -> bool:
    """A carga inicial já foi até o fim?

    Olha a última camada: a gold só existe depois que uma execução passou por
    todas as etapas. Se a carga inicial falhar no meio (na silver, por
    exemplo), a próxima execução faz a carga completa de novo, em vez de
    seguir só com o dia e deixar o histórico pela metade.
    """
    return DeltaTable.isDeltaTable(
        spark, storage.path("gold", config.PRODUTO, config.TABELA_RESUMO_DIARIO)
    )


def resolver_janela(spark: SparkSession, janela: Janela) -> Janela:
    """Janela efetiva: a pedida, ou todo o histórico se o produto ainda não foi carregado."""
    if janela.completa or produto_carregado(spark):
        return janela
    log.info("Produto ainda não carregado até a gold: a carga será completa.")
    return janela.como_completa()
