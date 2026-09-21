"""Testes da camada de dados (repo.py). Funcionam com pytest OU direto: python tests/test_repo.py

- Puros: sem rede.
- Integracao: usam o BigQuery de verdade com a chave da SA cobranca-app (GOOGLE_APPLICATION_CREDENTIALS).
  So mexem em dados de mentira (cliente -1, mes 1999-01) e limpam tudo no finally. Se ja existir um ciclo REAL
  iniciado, o teste que precisa criar ciclo e pulado, para nunca tocar em dado real.
"""
import os
import sys
from datetime import date

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import repo  # noqa: E402

SENTINELA = -1
MES_TESTE = date(1999, 1, 1)


# ---------- puros ----------

def test_normalizar_telefone():
    assert repo.normalizar_telefone("(11) 99999-9999") == "5511999999999"
    assert repo.normalizar_telefone("+55 11 99999-9999") == "5511999999999"
    assert repo.normalizar_telefone("11 3333-4444") == "551133334444"
    assert repo.normalizar_telefone("55 99999-9999") == "5555999999999"  # DDD 55 (RS) nao confunde com o pais
    assert repo.normalizar_telefone("") is None
    assert repo.normalizar_telefone(None) is None
    for ruim in ("123", "99999-999", "(11) 99999-99999999"):
        try:
            repo.normalizar_telefone(ruim)
        except ValueError:
            continue
        raise AssertionError(f"deveria rejeitar {ruim!r}")


def test_formatar_telefone():
    assert repo.formatar_telefone("5511999999999") == "(11) 99999-9999"
    assert repo.formatar_telefone("551133334444") == "(11) 3333-4444"
    assert repo.formatar_telefone(None) == ""


def test_brl():
    assert repo.brl(1234.5) == "R$ 1.234,50"
    assert repo.brl(0) == "R$ 0,00"
    assert repo.brl(None) == "R$ 0,00"
    assert repo.brl(2934.5) == "R$ 2.934,50"


def test_meses():
    assert repo.mes_sugerido(date(2026, 9, 21)) == date(2026, 8, 1)
    assert repo.mes_sugerido(date(2026, 1, 5)) == date(2025, 12, 1)
    assert repo.fim_do_mes(date(2026, 2, 1)) == date(2026, 2, 28)
    assert repo.fim_do_mes(date(2026, 8, 1)) == date(2026, 8, 31)
    assert repo.nome_mes(date(2026, 3, 1)) == "Março/2026"
    assert type(repo.mes_sugerido(date(2026, 9, 21))) is date


def test_resumo_por_estado():
    df = pd.DataFrame({"EstadoFila": ["A_COBRAR", "A_COBRAR", "COBRADO"], "TotalEmAberto": [100.0, 50.5, 10.0]})
    r = repo.resumo_por_estado(df)
    assert r["A_COBRAR"] == (2, 150.5) and r["COBRADO"] == (1, 10.0)
    assert r["INCOBRAVEL"] == (0, 0.0)
    assert repo.resumo_por_estado(pd.DataFrame(columns=["EstadoFila", "TotalEmAberto"]))["A_COBRAR"] == (0, 0.0)


# ---------- integracao (BigQuery real) ----------

def _cliente():
    return repo.get_client()


def _limpar(c):
    repo._q(c, f"DELETE FROM {repo.T_CICLOS} WHERE MesReferencia = @m", m=("DATE", MES_TESTE))
    repo._q(c, f"DELETE FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA))


def test_ciclo_idempotente():
    c = _cliente()
    try:
        assert repo.iniciar_ciclo(c, MES_TESTE) is True
        assert repo.iniciar_ciclo(c, MES_TESTE) is False, "iniciar duas vezes nao pode duplicar"
        n = list(repo._q(c, f"SELECT COUNT(*) AS n FROM {repo.T_CICLOS} WHERE MesReferencia = @m", m=("DATE", MES_TESTE)))[0]["n"]
        assert n == 1
        assert MES_TESTE in repo.ciclos_iniciados(c)
        try:
            repo.iniciar_ciclo(c, date(2026, 8, 15))
        except ValueError:
            pass
        else:
            raise AssertionError("mes que nao e dia 1 deveria ser rejeitado")
    finally:
        _limpar(c)


def test_contato_crud():
    c = _cliente()
    try:
        repo.salvar_contato(c, SENTINELA, "  Maria (filha) ", "(11) 99999-9999", " obs ", revisado=False)
        repo.salvar_contato(c, SENTINELA, "Maria (filha)", "(11) 99999-9999", "obs", revisado=False)  # MERGE: nao duplica
        df = repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe()
        assert len(df) == 1, "MERGE nao pode duplicar a linha"
        r = df.iloc[0]
        assert r["NomeContato"] == "Maria (filha)" and r["TelefoneWhatsapp"] == "5511999999999"
        assert r["Situacao"] == "ATIVO" and pd.isna(r["RevisadoEm"])
        repo.salvar_contato(c, SENTINELA, "Maria (filha)", "11 98888-7777", None, revisado=True)
        r = repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe().iloc[0]
        assert r["TelefoneWhatsapp"] == "5511988887777" and not pd.isna(r["RevisadoEm"]) and pd.isna(r["Observacao"])
        repo.definir_situacao(c, SENTINELA, "INCOBRAVEL")
        repo.definir_nao_cobrar(c, SENTINELA, date(2026, 8, 1))
        r = repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe().iloc[0]
        assert r["Situacao"] == "INCOBRAVEL" and r["NaoCobrarNoCiclo"] == date(2026, 8, 1)
        repo.definir_nao_cobrar(c, SENTINELA, None)
        r = repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe().iloc[0]
        assert pd.isna(r["NaoCobrarNoCiclo"])
        try:
            repo.definir_situacao(c, SENTINELA, "QUALQUER")
        except ValueError:
            pass
        else:
            raise AssertionError("situacao invalida deveria ser rejeitada")
        assert SENTINELA in repo.contatos_df(c)["CodigoCliente"].tolist()
    finally:
        _limpar(c)


def test_parametros_nao_sao_injecao():
    """Um nome com aspas e SQL dentro tem que ser gravado como texto, sem executar nada."""
    c = _cliente()
    try:
        perigoso = "x'); DELETE FROM x; --"
        repo.salvar_contato(c, SENTINELA, perigoso, None, None, revisado=False)
        r = repo._q(c, f"SELECT NomeContato FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe().iloc[0]
        assert r["NomeContato"] == perigoso
    finally:
        _limpar(c)


def test_contatos_trazem_animais():
    """So leitura: a aba Contatos mostra os animais do cliente numa linha, 'Mel / Thor', falecidos por ultimo com cruz."""
    df = repo.contatos_df(_cliente())
    assert "Animais" in df.columns
    assert df["Animais"].notna().mean() > 0.9, "quase todo contato tem animal cadastrado"
    com_varios = df["Animais"].dropna()[df["Animais"].dropna().str.contains(" / ", regex=False)]
    assert len(com_varios) > 0, "deveria haver cliente com 2 ou mais animais"
    for texto in df["Animais"].dropna():
        partes = texto.split(" / ")
        falecidos = [p.endswith(" †") for p in partes]
        assert falecidos == sorted(falecidos), f"falecidos devem ficar por ultimo: {texto!r}"


def test_garantir_contato_a_partir_do_cadastro():
    """definir_situacao num cliente SEM linha em contatos cria a linha a partir do cadastro do Simples Vet."""
    c = _cliente()
    codigo = list(repo._q(c, f"""
        SELECT CAST(Codigo AS INT64) AS Codigo FROM {repo.T_CLIENTS}
        WHERE CAST(Codigo AS INT64) NOT IN (SELECT CodigoCliente FROM {repo.T_CONTATOS}) LIMIT 1"""))[0]["Codigo"]
    try:
        repo.definir_situacao(c, codigo, "INCOBRAVEL")
        r = repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", codigo)).to_dataframe()
        assert len(r) == 1 and r.iloc[0]["Situacao"] == "INCOBRAVEL" and pd.isna(r.iloc[0]["RevisadoEm"])
        assert r.iloc[0]["NomeContato"], "deveria herdar o nome do cadastro"
    finally:
        repo._q(c, f"DELETE FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", codigo))


def test_fila_com_ciclo():
    c = _cliente()
    if repo.ciclos_iniciados(c):
        print("  (pulado: ja existe ciclo real iniciado)")
        return
    vazia = repo.fila(c)
    assert vazia.empty, "sem ciclo iniciado a fila tem que estar vazia"
    real = date(2026, 8, 1)
    try:
        assert repo.iniciar_ciclo(c, real)
        df = repo.fila(c)
        assert len(df) > 0 and df["CodigoCliente"].is_unique
        assert set(df["EstadoFila"]) <= set(repo.ESTADOS)
        esperado = float(list(repo._q(c, f"""
            SELECT SUM(Liquido) AS t FROM `{repo.PROJECT}.FisioVet.sales`
            WHERE Status IN ('Aberto','Baixa parcial') AND DATE(DataHora) < DATE '2026-09-01'"""))[0]["t"])
        assert abs(float(df["TotalEmAberto"].sum()) - esperado) < 0.005, "total da fila difere do calculo direto no sales"
        codigo = int(df.iloc[0]["CodigoCliente"])
        s = repo.sessoes_cliente(c, codigo)
        assert len(s) == int(df.iloc[0]["QtdSessoes"])
    finally:
        repo._q(c, f"DELETE FROM {repo.T_CICLOS} WHERE MesReferencia = @m", m=("DATE", real))


if __name__ == "__main__":
    falhas = 0
    for nome, fn in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
        try:
            fn()
            print(f"PASS  {nome}")
        except Exception as e:  # noqa: BLE001
            falhas += 1
            print(f"FAIL  {nome}: {type(e).__name__}: {e}")
    print(f"\n{'TUDO OK' if not falhas else str(falhas) + ' FALHA(S)'}")
    sys.exit(1 if falhas else 0)
