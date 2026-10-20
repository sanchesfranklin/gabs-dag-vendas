"""Regras de negócio dos jobs silver e gold, com dados em memória."""

from datetime import date, datetime, time
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")  # os jobs importam gabs.tables, que usa o Delta

from pyspark.sql.types import DateType, StructField, StructType  # noqa: E402

from vendas import config  # noqa: E402
from vendas.jobs.gold import resumir_por_dia  # noqa: E402
from vendas.jobs.silver import padronizar_pedidos  # noqa: E402
from vendas.schemas import SCHEMA_ORIGEM_PEDIDOS  # noqa: E402

# A bronze tem as colunas da origem, mais o dia de referência.
SCHEMA_BRONZE = StructType(
    [*SCHEMA_ORIGEM_PEDIDOS.fields, StructField(config.COLUNA_REFERENCIA, DateType())]
)

DIA_1 = date(2026, 9, 10)
DIA_2 = date(2026, 9, 11)


def pedido(id_pedido, uf="RN", valor="10.00", status="pago", dia=DIA_1):
    """Uma linha da bronze. Pedidos ao meio-dia, longe da virada do dia em qualquer fuso."""
    criado_em = datetime.combine(dia, time(12))
    return (id_pedido, f"Cliente {id_pedido}", uf, Decimal(valor), status, criado_em, dia)


def bronze(spark, *pedidos):
    return spark.createDataFrame(list(pedidos), SCHEMA_BRONZE)


def test_silver_usa_os_nomes_do_contrato(spark):
    silver = padronizar_pedidos(bronze(spark, pedido(1)))

    assert silver.columns == [
        "ID_PEDIDO", "NM_CLIENTE", "SG_UF", "VL_PEDIDO", "DS_STATUS", "DH_CRIACAO",
        config.COLUNA_REFERENCIA,
    ]


def test_silver_padroniza_uf_e_status(spark):
    linha = padronizar_pedidos(bronze(spark, pedido(1, uf=" rn ", status=" PAGO"))).first()

    assert linha["SG_UF"] == "RN"
    assert linha["DS_STATUS"] == "pago"


def test_silver_remove_pedidos_duplicados(spark):
    silver = padronizar_pedidos(bronze(spark, pedido(1), pedido(1), pedido(2)))

    assert sorted(linha["ID_PEDIDO"] for linha in silver.collect()) == [1, 2]


def test_gold_resume_so_pedidos_pagos_por_dia_e_uf(spark):
    silver = padronizar_pedidos(
        bronze(
            spark,
            pedido(1, uf="RN", valor="10.00"),
            pedido(2, uf="RN", valor="20.00"),
            pedido(3, uf="RN", valor="99.00", status="cancelado"),
            pedido(4, uf="CE", valor="5.00"),
            pedido(5, uf="RN", valor="7.50", dia=DIA_2),
        )
    )

    resumo = {
        (linha[config.COLUNA_DATA_PEDIDO], linha["SG_UF"]): (
            linha["QTD_PEDIDOS"], linha["VL_TOTAL"], linha["VL_TICKET_MEDIO"],
        )
        for linha in resumir_por_dia(silver).collect()
    }

    assert resumo == {
        (DIA_1, "RN"): (2, Decimal("30.00"), Decimal("15.00")),
        (DIA_1, "CE"): (1, Decimal("5.00"), Decimal("5.00")),
        (DIA_2, "RN"): (1, Decimal("7.50"), Decimal("7.50")),
    }
