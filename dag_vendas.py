"""DAG do produto vendas: origem -> landing -> bronze -> silver -> gold.

A primeira execução traz todo o histórico (carga completa). As seguintes
trazem só o dia do intervalo de dados da execução (carga incremental).

O arquivo só DESCREVE o fluxo: as tarefas, a ordem e o que cada uma passa
para a próxima. O que cada tarefa executa está em vendas/tarefas.py.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param

from vendas import tarefas
from vendas.config import FUSO, PRODUTO

with DAG(
    dag_id="vendas_pedidos",
    # Intervalos diários de meia-noite a meia-noite, no fuso de Brasília. A
    # execução que roda em 18/10 à 0h cobre o intervalo [17/10, 18/10), e a
    # data lógica dela é 17/10: o dia dos dados, não o dia em que rodou.
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone=FUSO),
    start_date=pendulum.datetime(2026, 9, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    # Aparece como formulário no "Trigger" da tela do Airflow
    # se quiser uma execução completa basta marcar o campo e disparar.
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
    tags=[PRODUTO, "lakehouse"],
) as dag:

    definir_janela = PythonOperator(
        task_id="definir_janela",
        python_callable=tarefas.definir_janela,
    )

    # .output é o retorno da tarefa (XCom), entregue à função como dict. Com
    # um template "{{ ti.xcom_pull(...) }}" chegaria como texto.
    landing = PythonOperator(
        task_id="landing",
        python_callable=tarefas.extrair_landing,
        op_kwargs={"janela": definir_janela.output},
    )

    # Se a função devolver False, as tarefas seguintes são puladas (ficam
    # "skipped", não "failed"). Com ignore_downstream_trigger_rules=False, só
    # a próxima é pulada direto; as demais seguem a própria regra, o que
    # permite à tarefa "fim" rodar mesmo assim.
    tem_pedidos = ShortCircuitOperator(
        task_id="tem_pedidos",
        python_callable=tarefas.tem_pedidos,
        op_kwargs={"carga": landing.output},
        ignore_downstream_trigger_rules=False,
    )

    # A landing decide a janela (diária ou completa) e entrega a MESMA janela
    # a todas as camadas.
    bronze = PythonOperator(
        task_id="bronze",
        python_callable=tarefas.carregar_bronze,
        op_kwargs={"carga": landing.output},
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=tarefas.tratar_silver,
        op_kwargs={"carga": landing.output},
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=tarefas.gerar_gold,
        op_kwargs={"carga": landing.output},
    )

    # Marca o fim da execução, com ou sem pedidos. "none_failed" faz ela rodar
    # quando as anteriores terminaram bem OU foram puladas, e não rodar se
    # alguma falhou.
    fim = EmptyOperator(task_id="fim", trigger_rule="none_failed")

    # O .output já liga cada camada à landing; a ordem completa fica explícita aqui.
    definir_janela >> landing >> tem_pedidos >> bronze >> silver >> gold >> fim
