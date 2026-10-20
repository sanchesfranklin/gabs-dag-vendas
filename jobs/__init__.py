"""Uma etapa do pipeline por módulo: landing, bronze, silver e gold.

Todo job segue o mesmo roteiro: abre a sessão Spark, lê a camada anterior,
aplica as regras da etapa e grava a fatia da janela na camada seguinte. A
entrada é sempre a função executar(janela).

As regras de negócio ficam numa função à parte no próprio módulo, que recebe
e devolve DataFrame, sem ler nem gravar nada. Assim podem ser testadas com
dados em memória, sem MinIO, Metastore ou banco de origem.

As tarefas da DAG importam os jobs dentro de si, na hora de rodar: este
pacote carrega o Spark e não deve ser importado ao ler a DAG.
"""
