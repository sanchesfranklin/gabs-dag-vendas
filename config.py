"""Configurações do produto vendas: nomes, convenções e acesso à origem.

Tudo o que o pipeline precisa saber sobre "como se chama" e "onde está" fica
aqui, num lugar só. O resto do código não lê variáveis de ambiente nem
repete nomes de tabelas e colunas.

Este módulo só usa a biblioteca padrão: a DAG o importa ao ser lida pelo
Airflow, e isso precisa ser rápido.
"""

import os
from dataclasses import dataclass, field

PRODUTO = "vendas"

# Fuso dos horários gravados na origem e do agendamento da DAG
FUSO = "America/Sao_Paulo"

# --- Origem: tabela no banco "fonte" do PostgreSQL ----------------------------
TABELA_ORIGEM = "vendas.pedidos"
# Coluna que data cada pedido na origem
COLUNA_DATA_ORIGEM = "criado_em"

# --- Lake ---------------------------------------------------------------------
TABELA_PEDIDOS = "pedidos"
TABELA_RESUMO_DIARIO = "resumo_diario"

# Partição da landing (em minúsculas, como as colunas da origem)
PARTICAO_LANDING = "dt_referencia"
# Dia de referência em bronze e silver
COLUNA_REFERENCIA = "DT_REFERENCIA"
# Dia do pedido na gold
COLUNA_DATA_PEDIDO = "DT_PEDIDO"

# Só pedidos pagos entram nos indicadores da gold
STATUS_PAGO = "pago"


@dataclass(frozen=True)
class ConexaoOrigem:
    """Acesso JDBC ao banco de origem."""

    url: str
    usuario: str
    # Fora do repr: a senha não aparece se a conexão for parar num log.
    senha: str = field(repr=False)
    driver: str = "org.postgresql.Driver"

    @classmethod
    def do_ambiente(cls) -> "ConexaoOrigem":
        """Lê a conexão das variáveis de ambiente, definidas no docker-compose."""
        return cls(
            url=_variavel("FONTE_JDBC_URL"),
            usuario=_variavel("FONTE_USER"),
            senha=_variavel("FONTE_PASSWORD"),
        )


def _variavel(nome: str) -> str:
    """Valor de uma variável de ambiente obrigatória, com erro claro se faltar."""
    valor = os.environ.get(nome)
    if not valor:
        raise RuntimeError(
            f"Variável de ambiente {nome} não definida. Confira o docker-compose e o .env."
        )
    return valor
