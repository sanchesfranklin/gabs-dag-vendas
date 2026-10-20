from datetime import date

import pytest

pytest.importorskip("pyspark")

from vendas.conectores.origem import consulta_pedidos  # noqa: E402
from vendas.janela import Janela  # noqa: E402


def test_consulta_da_origem_filtra_a_janela_no_banco():
    janela = Janela(inicio=date(2026, 10, 17), fim=date(2026, 10, 18))

    assert consulta_pedidos(janela) == (
        "(SELECT * FROM vendas.pedidos "
        "WHERE criado_em >= '2026-10-17' AND criado_em < '2026-10-18') AS pedidos_da_janela"
    )


def test_consulta_da_carga_completa_traz_todo_o_historico():
    janela = Janela(inicio=None, fim=date(2026, 10, 18))

    assert "WHERE criado_em < '2026-10-18'" in consulta_pedidos(janela)
