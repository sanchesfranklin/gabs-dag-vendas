"""Estrutura da DAG: tarefas e dependências. Roda onde o Airflow está instalado."""

import pytest

pytest.importorskip("airflow")

from vendas.dag_vendas import dag  # noqa: E402


def test_tarefas_da_dag():
    assert set(dag.task_ids) == {
        "definir_janela", "landing", "tem_pedidos", "bronze", "silver", "gold", "fim",
    }


@pytest.mark.parametrize(
    ("tarefa", "anteriores"),
    [
        ("landing", {"definir_janela"}),
        ("tem_pedidos", {"landing"}),
        # Além da etapa anterior, cada camada depende da landing, que entrega a janela.
        ("bronze", {"tem_pedidos", "landing"}),
        ("silver", {"bronze", "landing"}),
        ("gold", {"silver", "landing"}),
        ("fim", {"gold"}),
    ],
)
def test_ordem_das_tarefas(tarefa, anteriores):
    assert dag.get_task(tarefa).upstream_task_ids == anteriores


def test_fim_roda_mesmo_sem_pedidos():
    assert dag.get_task("fim").trigger_rule == "none_failed"
