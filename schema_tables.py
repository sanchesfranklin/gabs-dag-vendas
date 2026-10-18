"""Mapeamento de colunas: nome na origem -> (nome padronizado, tipo Spark).

É o contrato entre bronze e silver. A bronze guarda as colunas como vieram;
a silver aplica esta tabela para renomear (convenção de prefixos) e tipar.
"""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_SILVER_PEDIDOS = {
    "id_pedido": ("ID_PEDIDO", IntegerType()),
    "cliente": ("NM_CLIENTE", StringType()),
    "uf": ("SG_UF", StringType()),
    "valor": ("VL_PEDIDO", DecimalType(10, 2)),
    "status": ("DS_STATUS", StringType()),
    "criado_em": ("DH_CRIACAO", TimestampType()),
}

# Schema dos arquivos JSON da landing, com os nomes da origem. Ler com schema
# explícito evita que o Spark "adivinhe" tipos diferentes a cada dia e permite
# ler um dia sem pedidos (pasta sem dados) sem erro.
SCHEMA_ORIGEM_PEDIDOS = StructType(
    [StructField(origem, tipo) for origem, (_, tipo) in SCHEMA_SILVER_PEDIDOS.items()]
)
