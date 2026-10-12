"""Constantes do produto vendas."""

PRODUTO = "vendas"

# Origem: tabela no banco "fonte" do PostgreSQL
TABELA_ORIGEM = "vendas.pedidos"

# Nomes das tabelas no lake
TABELA_PEDIDOS = "pedidos"
TABELA_RESUMO_DIARIO = "resumo_diario"

# Só pedidos pagos entram nos indicadores da gold
STATUS_PAGO = "pago"
