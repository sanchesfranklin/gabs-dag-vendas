"""Fixtures compartilhadas pelos testes."""

import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    """Spark local e em memória: sem MinIO, sem Metastore e sem Delta.

    Basta para testar as transformações, que não leem nem gravam nada.
    """
    sql = pytest.importorskip("pyspark.sql")
    # Os processos Python do Spark usam o mesmo interpretador dos testes.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    sessao = (
        sql.SparkSession.builder.master("local[1]")
        .appName("vendas_testes")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield sessao
    sessao.stop()
