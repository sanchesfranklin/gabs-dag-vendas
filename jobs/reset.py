"""Manutenção: apaga as tabelas e os arquivos do produto em todas as camadas.

Uso pontual, para recomeçar do zero (por exemplo, quando a estrutura das
tabelas muda). A execução seguinte da DAG faz a carga completa. Não existe
desfazer.

    from vendas.jobs import reset
    reset.executar()
"""

from gabs import tables
from gabs.spark import get_spark_session
from vendas import config
from vendas.conectores import landing


def executar() -> None:
    """Remove o produto da bronze, silver e gold (catálogo e arquivos) e da landing."""
    spark = get_spark_session(f"{config.PRODUTO}_reset")
    try:
        tables.drop_table(spark, "bronze", config.PRODUTO, config.TABELA_PEDIDOS)
        tables.drop_table(spark, "silver", config.PRODUTO, config.TABELA_PEDIDOS)
        tables.drop_table(spark, "gold", config.PRODUTO, config.TABELA_RESUMO_DIARIO)
        tables.delete_path(spark, landing.caminho())
    finally:
        spark.stop()
