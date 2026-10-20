import pytest

from vendas.config import ConexaoOrigem


@pytest.fixture
def ambiente(monkeypatch):
    monkeypatch.setenv("FONTE_JDBC_URL", "jdbc:postgresql://postgres:5432/fonte")
    monkeypatch.setenv("FONTE_USER", "gabs")
    monkeypatch.setenv("FONTE_PASSWORD", "segredo")
    return monkeypatch


def test_conexao_lida_do_ambiente(ambiente):
    conexao = ConexaoOrigem.do_ambiente()

    assert conexao.url == "jdbc:postgresql://postgres:5432/fonte"
    assert conexao.usuario == "gabs"
    assert conexao.senha == "segredo"


def test_senha_nao_aparece_no_repr(ambiente):
    assert "segredo" not in repr(ConexaoOrigem.do_ambiente())


def test_variavel_faltando_gera_erro_claro(ambiente):
    ambiente.delenv("FONTE_PASSWORD")

    with pytest.raises(RuntimeError, match="FONTE_PASSWORD"):
        ConexaoOrigem.do_ambiente()
