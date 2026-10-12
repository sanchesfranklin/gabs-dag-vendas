# Produto: vendas

Pipeline de pedidos do Gabs Data Framework, da origem até a camada gold.

| Etapa | Origem | Destino |
|---|---|---|
| landing | PostgreSQL `fonte.vendas.pedidos` | `s3a://landing/vendas/pedidos/dt_carga=AAAA-MM-DD/` (JSON) |
| bronze | landing | `bronze.vendas_pedidos` (Delta) |
| silver | bronze | `silver.vendas_pedidos` (Delta, colunas padronizadas, sem duplicatas) |
| gold | silver | `gold.vendas_resumo_diario` (Delta, pedidos pagos por dia e UF) |

## Arquivos

- `dag_vendas.py`: a DAG do Airflow (só o fluxo).
- `jobs.py`: o código de cada etapa.
- `pipeline.py`: constantes do produto.
- `schema_tables.py`: mapeamento das colunas da origem para os nomes padronizados.

Este repositório é clonado dentro da pasta `dags/` do Gabs Data Framework, com o nome `vendas`:

```bash
git clone <url-deste-repositorio> dags/vendas
```
