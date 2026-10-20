"""Landing: os pedidos em JSON, como vieram da origem, numa pasta por dia.

    s3a://landing/vendas/pedidos/dt_referencia=AAAA-MM-DD/
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gabs import storage
from vendas import config
from vendas.janela import Janela
from vendas.schemas import SCHEMA_ORIGEM_PEDIDOS


def caminho() -> str:
    """Pasta da tabela na landing. Cada dia fica numa subpasta dt_referencia=AAAA-MM-DD."""
    return storage.path("landing", config.PRODUTO, config.TABELA_PEDIDOS)


def gravar_pedidos(df: DataFrame) -> None:
    """Grava os pedidos da origem, substituindo só os dias que vieram em df."""
    (
        # O dia de cada pedido vira a pasta onde ele é gravado. Com
        # partitionBy, a coluna sai do conteúdo do arquivo e fica só no
        # nome da pasta: o JSON continua igual ao da origem.
        df.withColumn(config.PARTICAO_LANDING, F.to_date(config.COLUNA_DATA_ORIGEM))
        .write.mode("overwrite")
        .partitionBy(config.PARTICAO_LANDING)
        # "dynamic": substitui só as pastas dos dias que vieram nesta
        # gravação. Sem isso, o overwrite apagaria a landing inteira.
        .option("partitionOverwriteMode", "dynamic")
        .json(caminho())
    )


def ler_pedidos(spark: SparkSession, janela: Janela) -> DataFrame:
    """Lê os dias da janela, já com a coluna DT_REFERENCIA.

    O Spark lê a pasta da tabela e descobre os dias pelos nomes das
    subpastas. O filtro na partição faz ele abrir só as pastas da janela.

    A partição vira DT_REFERENCIA num select, e não com withColumn + drop:
    o Spark não diferencia maiúsculas de minúsculas nos nomes de coluna, e
    drop("dt_referencia") apagaria também a DT_REFERENCIA.
    """
    return (
        spark.read.schema(SCHEMA_ORIGEM_PEDIDOS).json(caminho())
        .filter(janela.filtro(config.PARTICAO_LANDING))
        .select(
            *SCHEMA_ORIGEM_PEDIDOS.fieldNames(),
            F.col(config.PARTICAO_LANDING).cast("date").alias(config.COLUNA_REFERENCIA),
        )
    )
