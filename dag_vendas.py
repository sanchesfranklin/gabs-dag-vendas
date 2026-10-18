"""DAG do produto vendas: origem -> landing -> bronze -> silver -> gold.

Cada execução cuida de UM dia de pedidos: o intervalo de dados da execução.
O arquivo só DESCREVE o fluxo; o trabalho pesado fica em jobs.py, importado
dentro de cada tarefa, para o Airflow não carregar o Spark só para ler a DAG.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronDataIntervalTimetable, dag, task

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
    tags=["vendas", "lakehouse"],
)
def vendas_pedidos():

    @task
    def definir_dia(**context) -> str:
        """Converte o início do intervalo da execução no dia de referência (AAAA-MM-DD)."""
        inicio = context["data_interval_start"]
        if inicio is None:
            raise ValueError(
                "Execução sem intervalo de dados. Dispare com uma data lógica "
                "(Trigger com data) ou use backfill."
            )
        return inicio.in_timezone(FUSO).date().isoformat()

    @task
    def landing(dia: str) -> int:
        from vendas.jobs import extrair_landing

        return extrair_landing(dia)  # quantidade de pedidos do dia

    # short_circuit: se a função devolver False, as tarefas seguintes são
    # puladas (ficam "skipped", não "failed"). Com
    # ignore_downstream_trigger_rules=False, só a próxima é pulada direto; as
    # demais seguem a própria regra, o que permite à tarefa "fim" rodar mesmo
    # assim.
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def tem_pedidos(quantidade: int) -> bool:
        print(f"Pedidos no dia: {quantidade}")
        return quantidade > 0

    @task
    def bronze(dia: str) -> None:
        from vendas.jobs import carregar_bronze

        carregar_bronze(dia)

    @task
    def silver(dia: str) -> None:
        from vendas.jobs import tratar_silver

        tratar_silver(dia)

    @task
    def gold(dia: str) -> None:
        from vendas.jobs import gerar_gold

        gerar_gold(dia)

    # Marca o fim da execução, com ou sem pedidos. "none_failed" faz ela rodar
    # quando as anteriores terminaram bem OU foram puladas, e não rodar se
    # alguma falhou.
    fim = EmptyOperator(task_id="fim", trigger_rule="none_failed")

    # O dia é calculado uma vez e entregue (via XCom) a todas as etapas.
    dia = definir_dia()
    tem_pedidos(landing(dia)) >> bronze(dia) >> silver(dia) >> gold(dia) >> fim


vendas_pedidos()
