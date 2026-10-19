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

## Arquivos

- `dag_vendas.py`: a DAG do Airflow (só o fluxo).
- `jobs.py`: o código de cada etapa.
- `pipeline.py`: constantes do produto.
- `schema_tables.py`: mapeamento das colunas da origem para os nomes padronizados.

Este repositório é clonado dentro da pasta `dags/` do Gabs Data Framework, com o nome `vendas`:

```bash
git clone <url-deste-repositorio> dags/vendas
```
