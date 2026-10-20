"""Janela de carga: o intervalo de dias que uma execução processa.

Toda etapa do pipeline recebe a mesma janela [inicio, fim) e só toca essa
fatia de dados:

    execução diária   [dia, dia + 1)          só os pedidos do dia
    carga completa    [sem início, fim)       todo o histórico até o fim

Rodar a mesma janela de novo substitui exatamente a mesma fatia, com os mesmos
dados de origem. É isso que torna o pipeline idempotente.

Este módulo só usa a biblioteca padrão: a DAG o importa ao ser lida pelo
Airflow, e isso precisa ser rápido.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True, kw_only=True)
class Janela:
    """Intervalo de dias [inicio, fim). Sem início, cobre todo o histórico até o fim."""

    inicio: date | None
    fim: date

    def __post_init__(self) -> None:
        if self.inicio is not None and self.inicio > self.fim:
            raise ValueError(f"Janela invertida: início {self.inicio} depois do fim {self.fim}.")

    @classmethod
    def do_intervalo(cls, inicio: datetime, fim: datetime, fuso: str) -> Janela:
        """Converte o intervalo de dados do Airflow nos dias correspondentes no fuso.

        O intervalo [17/10 00h, 18/10 00h) em Brasília chega em UTC, como
        [17/10 03h, 18/10 03h); a janela resultante é [17/10, 18/10).
        """
        tz = ZoneInfo(fuso)
        return cls(inicio=inicio.astimezone(tz).date(), fim=fim.astimezone(tz).date())

    @property
    def completa(self) -> bool:
        """True se a janela cobre todo o histórico até o fim."""
        return self.inicio is None

    def como_completa(self) -> Janela:
        """A mesma janela, sem início: todo o histórico até o fim."""
        return replace(self, inicio=None)

    def filtro(self, coluna: str) -> str:
        """Condição SQL que seleciona a janela na coluna informada.

        >>> Janela(inicio=date(2026, 9, 10), fim=date(2026, 9, 11)).filtro("DT_REFERENCIA")
        "DT_REFERENCIA >= '2026-09-10' AND DT_REFERENCIA < '2026-09-11'"
        >>> Janela(inicio=None, fim=date(2026, 10, 18)).filtro("DT_REFERENCIA")
        "DT_REFERENCIA < '2026-10-18'"
        """
        condicao = f"{coluna} < '{self.fim.isoformat()}'"
        if self.inicio is not None:
            condicao = f"{coluna} >= '{self.inicio.isoformat()}' AND {condicao}"
        return condicao

    def para_dict(self) -> dict[str, str | None]:
        """Formato serializável, para a janela trafegar entre tarefas via XCom."""
        return {
            "inicio": self.inicio.isoformat() if self.inicio else None,
            "fim": self.fim.isoformat(),
        }

    @classmethod
    def de_dict(cls, dados: Mapping[str, Any]) -> Janela:
        """Reconstrói a janela de um para_dict. Chaves a mais são ignoradas."""
        inicio = dados.get("inicio")
        return cls(
            inicio=date.fromisoformat(inicio) if inicio else None,
            fim=date.fromisoformat(dados["fim"]),
        )

    def __str__(self) -> str:
        return f"[{self.inicio or 'início do histórico'}, {self.fim})"
