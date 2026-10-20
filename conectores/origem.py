"""Origem: a tabela de pedidos no PostgreSQL (banco "fonte"), lida por JDBC."""

from pyspark.sql import DataFrame, SparkSession

from vendas import config
from vendas.config import ConexaoOrigem
from vendas.janela import Janela


def consulta_pedidos(janela: Janela) -> str:
    """Subconsulta que a origem executa: só os pedidos criados dentro da janela.

    Em vez do nome da tabela, o JDBC recebe uma subconsulta: o filtro roda no
    PostgreSQL, e só os pedidos da janela trafegam pela rede.
    """
    return (
        f"(SELECT * FROM {config.TABELA_ORIGEM} "
        f"WHERE {janela.filtro(config.COLUNA_DATA_ORIGEM)}) AS pedidos_da_janela"
    )


def ler_pedidos(spark: SparkSession, janela: Janela, conexao: ConexaoOrigem) -> DataFrame:
    """Pedidos da janela, exatamente como estão na origem."""
    return (
        spark.read.format("jdbc")
        .option("url", conexao.url)
        .option("dbtable", consulta_pedidos(janela))
        .option("user", conexao.usuario)
        .option("password", conexao.senha)
        .option("driver", conexao.driver)
        .load()
    )
