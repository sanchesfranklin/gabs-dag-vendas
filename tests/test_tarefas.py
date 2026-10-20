"""Funções chamadas pelas tarefas da DAG. Não precisam do Airflow nem do Spark."""

from datetime import datetime, timezone

import pytest

from vendas import tarefas

# [17/10 00h, 18/10 00h) em Brasília, como o Airflow entrega: em UTC.
INICIO = datetime(2026, 10, 17, 3, tzinfo=timezone.utc)
FIM = datetime(2026, 10, 18, 3, tzinfo=timezone.utc)


def test_janela_diaria():
    janela = tarefas.definir_janela(INICIO, FIM, {"carga_completa": False})

    assert janela == {"inicio": "2026-10-17", "fim": "2026-10-18"}


def test_janela_com_carga_completa_nao_tem_inicio():
    janela = tarefas.definir_janela(INICIO, FIM, {"carga_completa": True})

    assert janela == {"inicio": None, "fim": "2026-10-18"}


def test_execucao_sem_intervalo_falha():
    with pytest.raises(ValueError, match="sem intervalo"):
        tarefas.definir_janela(None, None, {"carga_completa": False})


@pytest.mark.parametrize(("quantidade", "esperado"), [(0, False), (12, True)])
def test_tem_pedidos(quantidade, esperado):
    carga = {"inicio": "2026-10-17", "fim": "2026-10-18", "quantidade": quantidade}

    assert tarefas.tem_pedidos(carga) is esperado
