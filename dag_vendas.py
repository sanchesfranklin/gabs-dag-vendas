"""DAG do produto vendas: origem -> landing -> bronze -> silver -> gold.

O arquivo só DESCREVE o fluxo. O trabalho pesado fica em jobs.py, importado
dentro de cada tarefa: assim o Airflow, que relê este arquivo a cada poucos
segundos, não precisa carregar o Spark só para desenhar o grafo.
"""

from datetime import date, datetime

from airflow.sdk import dag, task


@dag(
    dag_id="vendas_pedidos",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    # Uma tarefa por vez: cada uma abre um Spark, e a máquina tem 8 GB.
    max_active_tasks=1,
    tags=["vendas", "lakehouse"],
)
def vendas_pedidos():

    @task
    def landing(**context) -> str:
        from vendas.jobs import extrair_landing

        # "ds" é a data lógica da execução (AAAA-MM-DD). Numa execução manual
        # sem data, usa a de hoje.
        data_carga = context.get("ds") or date.today().isoformat()
        extrair_landing(data_carga)
        return data_carga  # vira XCom e é entregue à próxima tarefa

    @task
    def bronze(data_carga: str) -> None:
        from vendas.jobs import carregar_bronze

        carregar_bronze(data_carga)

    @task
    def silver() -> None:
        from vendas.jobs import tratar_silver

        tratar_silver()

    @task
    def gold() -> None:
        from vendas.jobs import gerar_gold

        gerar_gold()

    bronze(landing()) >> silver() >> gold()


vendas_pedidos()
