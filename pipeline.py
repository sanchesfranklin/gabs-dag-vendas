"""Constantes do produto vendas."""

PRODUTO = "vendas"

# Origem: tabela no banco "fonte" do PostgreSQL e a coluna que data cada pedido
TABELA_ORIGEM = "vendas.pedidos"
COLUNA_DATA_ORIGEM = "criado_em"

# Nomes das tabelas no lake
TABELA_PEDIDOS = "pedidos"
TABELA_RESUMO_DIARIO = "resumo_diario"

# Coluna que identifica o dia de referência em bronze e silver
COLUNA_REFERENCIA = "DT_REFERENCIA"

# Só pedidos pagos entram nos indicadores da gold
STATUS_PAGO = "pago"

# Fuso dos horários gravados na origem
FUSO = "America/Sao_Paulo"
