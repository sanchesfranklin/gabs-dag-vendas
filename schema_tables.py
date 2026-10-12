"""Mapeamento de colunas: nome na origem -> (nome padronizado, tipo Spark).

É o contrato entre bronze e silver. A bronze guarda as colunas como vieram;
a silver aplica esta tabela para renomear (convenção de prefixos) e tipar.
"""

from pyspark.sql.types import DecimalType, IntegerType, StringType, TimestampType

PEDIDOS = {
    "id_pedido": ("ID_PEDIDO", IntegerType()),
    "cliente": ("NM_CLIENTE", StringType()),
    "uf": ("SG_UF", StringType()),
    "valor": ("VL_PEDIDO", DecimalType(10, 2)),
    "status": ("DS_STATUS", StringType()),
    "criado_em": ("DH_CRIACAO", TimestampType()),
}
