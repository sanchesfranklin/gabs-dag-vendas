from datetime import date, datetime, timezone

import pytest

from vendas.janela import Janela

DIA = Janela(inicio=date(2026, 10, 17), fim=date(2026, 10, 18))


def test_filtro_da_janela_diaria():
    assert DIA.filtro("DT_REFERENCIA") == (
        "DT_REFERENCIA >= '2026-10-17' AND DT_REFERENCIA < '2026-10-18'"
    )


def test_filtro_da_janela_completa_nao_tem_inicio():
    assert DIA.como_completa().filtro("DT_REFERENCIA") == "DT_REFERENCIA < '2026-10-18'"


def test_como_completa_mantem_o_fim():
    completa = DIA.como_completa()

    assert completa.completa
    assert completa.fim == DIA.fim
    assert not DIA.completa


def test_intervalo_em_utc_vira_dias_no_fuso():
    # [17/10 00h, 18/10 00h) em Brasília chega ao Airflow em UTC.
    janela = Janela.do_intervalo(
        datetime(2026, 10, 17, 3, tzinfo=timezone.utc),
        datetime(2026, 10, 18, 3, tzinfo=timezone.utc),
        "America/Sao_Paulo",
    )

    assert janela == DIA


@pytest.mark.parametrize("janela", [DIA, DIA.como_completa()])
def test_ida_e_volta_pelo_xcom(janela):
    assert Janela.de_dict(janela.para_dict()) == janela


def test_de_dict_ignora_chaves_a_mais():
    carga = {"inicio": "2026-10-17", "fim": "2026-10-18", "quantidade": 12}

    assert Janela.de_dict(carga) == DIA


def test_janela_invertida_e_rejeitada():
    with pytest.raises(ValueError, match="invertida"):
        Janela(inicio=date(2026, 10, 18), fim=date(2026, 10, 17))
