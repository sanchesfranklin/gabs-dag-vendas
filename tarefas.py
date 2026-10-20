"""Funções que as tarefas da DAG executam: a ponte entre o Airflow e os jobs.

Cada função recebe o que o Airflow entrega (variáveis do contexto da execução
e XComs, como dict), converte em Janela, chama o job da etapa e devolve um
dict para o próximo XCom. Não há regra de dados aqui.

A DAG importa este módulo ao ser lida pelo Airflow, então ele só usa a
biblioteca padrão: os jobs, que carregam o Spark, são importados dentro de
cada função, na hora de rodar.
"""

import logging
from datetime import datetime
from typing import Any

from vendas.config import FUSO
from vendas.janela import Janela

log = logging.getLogger(__name__)


def definir_janela(
    data_interval_start: datetime | None,
    data_interval_end: datetime | None,
    params: dict[str, Any],
) -> dict:
    """Janela pedida: o dia do intervalo da execução, ou todo o histórico se carga_completa.

    O Airflow preenche os argumentos pelo nome, com as variáveis do contexto
    da execução.
    """
    if data_interval_start is None or data_interval_end is None:
        raise ValueError(
            "Execução sem intervalo de dados. Dispare com uma data lógica "
            "(Trigger com data) ou use backfill."
        )
    janela = Janela.do_intervalo(data_interval_start, data_interval_end, FUSO)
    if params["carga_completa"]:
        janela = janela.como_completa()
    return janela.para_dict()


def extrair_landing(janela: dict) -> dict:
    """Extrai a janela e devolve a janela EFETIVA (sem início, se a carga foi completa)."""
    from vendas.jobs import landing as job

    efetiva, quantidade = job.executar(Janela.de_dict(janela))
    return {**efetiva.para_dict(), "quantidade": quantidade}


def tem_pedidos(carga: dict) -> bool:
    """True se a landing extraiu algum pedido; False faz a DAG pular as camadas."""
    log.info("Pedidos na janela: %d", carga["quantidade"])
    return carga["quantidade"] > 0


def carregar_bronze(carga: dict) -> None:
    from vendas.jobs import bronze as job

    job.executar(Janela.de_dict(carga))


def tratar_silver(carga: dict) -> None:
    from vendas.jobs import silver as job

    job.executar(Janela.de_dict(carga))


def gerar_gold(carga: dict) -> None:
    from vendas.jobs import gold as job

    job.executar(Janela.de_dict(carga))
