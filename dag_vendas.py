"""DAG do produto vendas: origem -> landing -> bronze -> silver -> gold.

A primeira execução traz todo o histórico (carga completa). As seguintes
trazem só o dia do intervalo de dados da execução (carga incremental).
O arquivo só DESCREVE o fluxo; o trabalho pesado fica em jobs.py, importado
dentro de cada tarefa, para o Airflow não carregar o Spark só para ler a DAG.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronDataIntervalTimetable, Param, dag, task

FUSO = "America/Sao_Paulo"


@dag(
    dag_id="vendas_pedidos",
    # Intervalos diários de meia-noite a meia-noite, no fuso de Brasília. A
    # execução que roda em 18/10 à 0h cobre o intervalo [17/10, 18/10), e a
    # data lógica dela é 17/10: o dia dos dados, não o dia em que rodou.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=FUSO),
    start_date=pendulum.datetime(2026, 9, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    # Uma tarefa por vez: cada uma abre um Spark, e a máquina tem 8 GB.
    max_active_tasks=1,
    # Aparece como formulário no "Trigger" da tela do Airflow.
    params={
        "carga_completa": Param(
            False,
            type="boolean",
            title="Carga completa",
            description=(
                "Recarrega todo o histórico até o fim do intervalo. A primeira "
                "carga já é completa sozinha; use para refazer tudo depois."
            ),
        ),
    },
    tags=["vendas", "lakehouse"],
)
def vendas_pedidos():

    @task
    def definir_janela(**context) -> dict:
        """Converte o intervalo da execução em datas (AAAA-MM-DD) no fuso de Brasília."""
        inicio = context["data_interval_start"]
        fim = context["data_interval_end"]
        if inicio is None or fim is None:
            raise ValueError(
                "Execução sem intervalo de dados. Dispare com uma data lógica "
                "(Trigger com data) ou use backfill."
            )
        return {
            "inicio": inicio.in_timezone(FUSO).date().isoformat(),
            "fim": fim.in_timezone(FUSO).date().isoformat(),
            "carga_completa": bool(context["params"]["carga_completa"]),
        }

    @task
    def landing(janela: dict) -> dict:
        """Extrai a janela e devolve a janela EFETIVA (sem início, se a carga foi completa)."""
        from vendas.jobs import extrair_landing

        return extrair_landing(janela["inicio"], janela["fim"], janela["carga_completa"])

    # short_circuit: se a função devolver False, as tarefas seguintes são
    # puladas (ficam "skipped", não "failed"). Com
    # ignore_downstream_trigger_rules=False, só a próxima é pulada direto; as
    # demais seguem a própria regra, o que permite à tarefa "fim" rodar mesmo
    # assim.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def tem_pedidos(carga: dict) -> bool:
        print(f"Pedidos na janela: {carga['quantidade']}")
        return carga["quantidade"] > 0

    @task
    def bronze(carga: dict) -> None:
        from vendas.jobs import carregar_bronze

        carregar_bronze(carga["inicio"], carga["fim"])

    @task
    def silver(carga: dict) -> None:
        from vendas.jobs import tratar_silver

        tratar_silver(carga["inicio"], carga["fim"])

    @task
    def gold(carga: dict) -> None:
        from vendas.jobs import gerar_gold

        gerar_gold(carga["inicio"], carga["fim"])

    # Marca o fim da execução, com ou sem pedidos. "none_failed" faz ela rodar
    # quando as anteriores terminaram bem OU foram puladas, e não rodar se
    # alguma falhou.
    fim = EmptyOperator(task_id="fim", trigger_rule="none_failed")

    # A landing decide a janela (diária ou completa) e entrega a MESMA janela,
    # via XCom, a todas as etapas seguintes.
    carga = landing(definir_janela())
    tem_pedidos(carga) >> bronze(carga) >> silver(carga) >> gold(carga) >> fim


vendas_pedidos()
