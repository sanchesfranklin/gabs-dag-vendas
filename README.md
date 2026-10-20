# Produto: vendas

Pipeline de pedidos do Gabs Data Framework, da origem até a camada gold.
A primeira execução traz todo o histórico; as seguintes trazem só o dia do intervalo da execução.
Em qualquer caso a execução substitui só a fatia de dias que processou, então repetir uma execução não duplica dados.

## Carga completa e incremental

| Situação | Janela processada |
|---|---|
| Produto ainda não carregado até a gold | Todos os pedidos criados antes do fim do intervalo |
| Produto já carregado | Só o dia do intervalo, `[dia, dia + 1)` |
| Trigger com o parâmetro `carga_completa` marcado | Todos os pedidos criados antes do fim do intervalo |

A decisão é tomada na tarefa `landing`, que entrega a mesma janela às etapas seguintes.
Se não houver pedidos na janela, `bronze`, `silver` e `gold` são puladas e a execução termina na tarefa `fim`.

| Etapa | Origem | Destino |
|---|---|---|
| landing | PostgreSQL `fonte.vendas.pedidos` | `s3a://landing/vendas/pedidos/dt_referencia=AAAA-MM-DD/` (JSON como veio da origem, uma pasta por dia) |
| bronze | landing | `bronze.vendas_pedidos` (Delta, partição `DT_REFERENCIA`) |
| silver | bronze | `silver.vendas_pedidos` (Delta, partição `DT_REFERENCIA`, colunas padronizadas, sem duplicatas) |
| gold | silver | `gold.vendas_resumo_diario` (Delta, partição `DT_PEDIDO`, pedidos pagos por dia e UF) |

## Estrutura

Cada parte do código tem uma responsabilidade só, e as dependências andam num sentido só: a DAG declara os operadores, os operadores chamam as funções de `tarefas`, que chamam os jobs, e os jobs usam os conectores. Todos usam `config` e `janela`.

```
vendas/
├── dag_vendas.py          # orquestração: operadores, ordem, agendamento e parâmetros
├── tarefas.py             # funções chamadas pelos operadores: XCom -> Janela -> job
├── config.py              # nomes de tabelas e colunas, fuso e conexão com a origem
├── janela.py              # Janela [inicio, fim): o que cada execução processa
├── schemas.py             # contrato de colunas: origem -> nomes padronizados e tipos
├── conectores/            # leitura e gravação fora do catálogo
│   ├── origem.py          #   PostgreSQL, por JDBC, filtrando a janela no banco
│   └── landing.py         #   arquivos JSON da landing, uma pasta por dia
├── jobs/                  # uma etapa por módulo, todas com executar(janela)
│   ├── landing.py         #   decide a janela efetiva e extrai da origem
│   ├── bronze.py
│   ├── silver.py          #   regra: padronizar_pedidos
│   ├── gold.py            #   regra: resumir_por_dia
│   └── reset.py           #   manutenção: apaga o produto em todas as camadas
└── tests/                 # testes da janela, da configuração, das tarefas, das regras e da DAG
```

| Camada do código | Pode importar | Não pode |
|---|---|---|
| `dag_vendas.py` | `tarefas`, `config` | Spark; regras de dados |
| `tarefas.py` | `config`, `janela`; os `jobs` só dentro das funções | Spark no topo do arquivo; Airflow |
| `jobs/` | `conectores`, `config`, `janela`, `schemas`, `gabs` | Airflow |
| `conectores/` | `config`, `janela`, `schemas`, `gabs` | regras de negócio |
| `config.py`, `janela.py` | só a biblioteca padrão | Spark e Airflow |

O Airflow lê o `dag_vendas.py` (e o que ele importa) a cada poucos segundos. Por isso `tarefas.py` importa os jobs só dentro de cada função: o Spark é carregado quando a tarefa roda, não quando a DAG é lida.

Os operadores recebem o resultado da tarefa anterior por `op_kwargs={"carga": landing.output}`. O `.output` entrega o XCom como dict e cria a dependência entre as tarefas; um template `"{{ ti.xcom_pull(...) }}"` entregaria o mesmo valor como texto.

Dentro de cada job, a regra de negócio fica numa função à parte, que recebe e devolve DataFrame sem ler nem gravar nada; o `executar` só lê, chama a regra e grava. É o que permite testar as regras sem a plataforma no ar.

### Como acrescentar uma etapa

1. Crie `jobs/<etapa>.py` com a regra numa função própria (DataFrame -> DataFrame) e teste-a em `tests/test_regras.py`.
2. No mesmo módulo, escreva `executar(janela)`: abrir a sessão com `get_spark_session`, ler, aplicar a regra, gravar com `replace_where=janela.filtro(...)` e encerrar a sessão com `spark.stop()` num `finally`.
3. Em `tarefas.py`, crie a função que converte o XCom em `Janela` e chama o job, importando-o dentro da função.
4. Na DAG, crie o `PythonOperator` com `python_callable=tarefas.<funcao>` e `op_kwargs={"carga": landing.output}`, e encaixe-o na linha de dependências.
5. Se o novo módulo não for uma DAG, a pasta dele já está no `.airflowignore`; se for um arquivo solto na raiz, acrescente-o lá.

### Recomeçar do zero

Para apagar o produto em todas as camadas (por exemplo, quando a estrutura das tabelas muda), dentro do contêiner do Airflow:

```bash
docker exec -it -w /opt/airflow/dags gabs-airflow python -c "from vendas.jobs import reset; reset.executar()"
```

A execução seguinte da DAG faz a carga completa. Não existe desfazer.

## Testes

A janela e as funções de `tarefas.py` são testadas em Python puro; as regras de negócio, com Spark local, em memória. Nenhum teste precisa de MinIO, Metastore ou banco de origem.
Os testes da DAG só rodam onde o Airflow está instalado; nos outros lugares, são pulados.

No contêiner do Airflow, que já tem Spark e Airflow (os testes não gravam nada nas pastas montadas):

```bash
docker exec gabs-airflow pip install pytest
docker exec -w /opt/airflow/dags gabs-airflow python -m pytest -p no:cacheprovider vendas/tests
```

Fora do contêiner, num ambiente com Python 3.11 e Java 17, a partir da pasta `dags/` (o `PYTHONPATH` torna a biblioteca `gabs` importável, como no contêiner):

```bash
pip install -r vendas/requirements-dev.txt
PYTHONPATH=.. python -m pytest vendas/tests
```

## Instalação

Este repositório é clonado dentro da pasta `dags/` do Gabs Data Framework, com o nome `vendas`:

```bash
git clone <url-deste-repositorio> dags/vendas
```
