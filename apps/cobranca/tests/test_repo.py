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


def test_rotulos_de_nota_fiscal():
    assert repo.rotulo_nf("COM_CPF") == "Fazer NF com CPF"
    assert repo.rotulo_nf("SEM_CPF") == "Fazer NF sem CPF"
    assert repo.rotulo_nf(None) is None and repo.rotulo_nf(float("nan")) is None and repo.rotulo_nf("") is None
    assert repo.nf_do_rotulo("Fazer NF com CPF") == "COM_CPF"
    assert repo.nf_do_rotulo("Fazer NF sem CPF") == "SEM_CPF"
    for vazio in (None, "", repo.NF_SEM, float("nan")):
        assert repo.nf_do_rotulo(vazio) is None
    try:
        repo.nf_do_rotulo("qualquer coisa")
    except ValueError:
        pass
    else:
        raise AssertionError("rotulo invalido deveria ser rejeitado")


def _sessoes(*linhas):
    return pd.DataFrame(linhas, columns=["DataSessao", "NomeAnimal", "ProdutoServico", "Valor", "Parcial"])


def test_juntar_lista_e_meses():
    assert repo.juntar_lista([]) == "" and repo.juntar_lista(["Mel"]) == "Mel"
    assert repo.juntar_lista(["Mel", "Thor"]) == "Mel e Thor"
    assert repo.juntar_lista(["A", "B", "C"]) == "A, B e C"
    assert repo.juntar_lista(["Mel", "", None]) == "Mel", "vazio e None nao viram texto ('None')"
    assert repo.mes_extenso(date(2026, 8, 1)) == "agosto de 2026"
    assert repo.mes_extenso(date(2026, 3, 1)) == "março de 2026"


def test_marcadores_desconhecidos():
    assert repo.marcadores_desconhecidos("Oi {nome_contato}, {total} {mes} {animais} {lista_sessoes} {nome_cliente}") == []
    assert repo.marcadores_desconhecidos("Chave {pix} e {nome_contat} e {pix}") == ["pix", "nome_contat"]
    assert repo.marcadores_desconhecidos("") == [] and repo.marcadores_desconhecidos(None) == []


def test_formatar_sessoes_um_mes():
    s = _sessoes((date(2026, 8, 12), "Thor", "Acupuntura", 150, True), (date(2026, 8, 5), "Mel", "Fisioterapia", 120, False))
    assert repo.formatar_sessoes(s) == ("05/08 - Mel - Fisioterapia - R$ 120,00\n"
                                        "12/08 - Thor - Acupuntura - R$ 150,00 (baixa parcial)")
    assert repo.formatar_sessoes(_sessoes()) == ""


def test_formatar_sessoes_varios_meses_com_subtotal():
    s = _sessoes((date(2026, 8, 5), "Mel", "Fisioterapia", 120, False), (date(2026, 7, 10), "Mel", "Fisioterapia", 100.5, False),
                 (date(2026, 7, 20), "Mel", "Fisioterapia", 100, False))
    esperado = ("*Julho/2026*\n10/07 - Mel - Fisioterapia - R$ 100,50\n20/07 - Mel - Fisioterapia - R$ 100,00\n"
                "Subtotal: R$ 200,50\n\n"
                "*Agosto/2026*\n05/08 - Mel - Fisioterapia - R$ 120,00\nSubtotal: R$ 120,00")
    assert repo.formatar_sessoes(s) == esperado


def test_montar_contexto_e_renderizar():
    s = _sessoes((date(2026, 8, 5), "Thor", "Fisioterapia", 120, False), (date(2026, 8, 6), "Mel", "Acupuntura", 80, False))
    linha = {"NomeContato": "Maria", "NomeCliente": "Maria da Silva", "MesCiclo": date(2026, 8, 1), "TotalEmAberto": 200}
    ctx = repo.montar_contexto(linha, s)
    assert ctx["animais"] == "Mel e Thor" and ctx["mes"] == "agosto de 2026" and ctx["total"] == "R$ 200,00"
    texto = "Bom dia, {nome_contato}! Sessões de {animais} até {mes}:\n{lista_sessoes}\n*Total: {total}* {pix}"
    r = repo.renderizar_mensagem(texto, ctx)
    assert r.startswith("Bom dia, Maria! Sessões de Mel e Thor até agosto de 2026:\n05/08 - Thor")
    assert "*Total: R$ 200,00*" in r and "{pix}" in r, "marcador desconhecido fica como esta (aparece na conferencia)"
    # valor com chaves nao e reprocessado (uma passada so)
    assert repo.renderizar_mensagem("{nome_contato}", {"nome_contato": "{total}", "total": "X"}) == "{total}"


def test_link_whatsapp():
    url, com_texto = repo.link_whatsapp("5511999999999", "Olá!\n*Total*: R$ 1,00")
    assert com_texto and url.startswith("https://web.whatsapp.com/send?phone=5511999999999&text="), "abre direto no WhatsApp Web"
    assert "wa.me" not in url
    assert "%0A" in url and "%2A" in url and "%C3%A1" in url, "quebra de linha, asterisco e acento precisam ir codificados"
    assert " " not in url and "\n" not in url
    longo, incluido = repo.link_whatsapp("5511999999999", "x" * 3000)
    base = "https://web.whatsapp.com/send?phone=5511999999999"
    assert longo == base and incluido is False, "texto longo: link sem texto"
    assert repo.link_whatsapp("5511999999999", None) == (base, False)


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
    repo._q(c, f"DELETE FROM {repo.T_ENVIOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA))


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


def test_nota_fiscal():
    c = _cliente()
    ciclo = date(2026, 8, 1)

    def linha():
        return repo._q(c, f"SELECT * FROM {repo.T_CONTATOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe().iloc[0]

    try:
        repo.salvar_contato(c, SENTINELA, "Teste", None, None, revisado=False)
        assert repo.definir_nf_emitida(c, SENTINELA, ciclo) == 0, "cliente sem NF configurada: marcar emitida deve ser ignorado"
        assert pd.isna(linha()["NFEmitidaNoCiclo"])
        repo.definir_nota_fiscal(c, SENTINELA, "COM_CPF")
        assert linha()["NotaFiscal"] == "COM_CPF"
        assert repo.definir_nf_emitida(c, SENTINELA, ciclo) == 1
        assert linha()["NFEmitidaNoCiclo"] == ciclo
        assert repo.definir_nf_emitida(c, SENTINELA, None) == 1, "desmarcar tem que funcionar"
        assert pd.isna(linha()["NFEmitidaNoCiclo"])
        repo.definir_nf_emitida(c, SENTINELA, ciclo)
        repo.definir_nota_fiscal(c, SENTINELA, "SEM_CPF")
        assert linha()["NFEmitidaNoCiclo"] == ciclo, "trocar COM_CPF por SEM_CPF nao apaga a emissao"
        repo.definir_nota_fiscal(c, SENTINELA, None)
        r = linha()
        assert pd.isna(r["NotaFiscal"]) and pd.isna(r["NFEmitidaNoCiclo"]), "tirar a NF tambem limpa a marca de emitida"
        try:
            repo.definir_nota_fiscal(c, SENTINELA, "XPTO")
        except ValueError:
            pass
        else:
            raise AssertionError("valor invalido deveria ser rejeitado")
        assert "NotaFiscal" in repo.contatos_df(c).columns
    finally:
        _limpar(c)


def test_envios_registrar_e_desfazer():
    c = _cliente()
    try:
        id1 = repo.registrar_envio(c, SENTINELA, MES_TESTE, "Inicial", "Maria", "5511999999999", 630.5, "texto 1 — ação")
        id2 = repo.registrar_envio(c, SENTINELA, MES_TESTE, "Lembrete", "Maria", "5511999999999", 630.5, "texto 2")
        assert id1 != id2
        df = repo._q(c, f"SELECT * FROM {repo.T_ENVIOS} WHERE CodigoCliente = @k ORDER BY EnviadoEm", k=("INT64", SENTINELA)).to_dataframe()
        assert len(df) == 2 and df.iloc[0]["MensagemNome"] == "Inicial" and df.iloc[0]["TextoEnviado"] == "texto 1 — ação"
        assert float(df.iloc[0]["ValorNoEnvio"]) == 630.5 and df.iloc[0]["MesReferencia"] == MES_TESTE
        assert repo.desfazer_ultimo_envio(c, SENTINELA, MES_TESTE) == 1
        resto = repo._q(c, f"SELECT MensagemNome FROM {repo.T_ENVIOS} WHERE CodigoCliente = @k", k=("INT64", SENTINELA)).to_dataframe()
        assert resto["MensagemNome"].tolist() == ["Inicial"], "desfazer apaga o MAIS RECENTE"
        assert repo.desfazer_ultimo_envio(c, SENTINELA, MES_TESTE) == 1
        assert repo.desfazer_ultimo_envio(c, SENTINELA, MES_TESTE) == 0, "sem envios: nada a desfazer"
        assert repo.desfazer_ultimo_envio(c, SENTINELA, date(1999, 2, 1)) == 0, "outro ciclo nao e afetado"
    finally:
        _limpar(c)


def test_mensagens_crud_e_uma_inicial_so():
    c = _cliente()
    tabela = f"`{repo.PROJECT}.FisioVet_App.mensagens_teste`"   # copia descartavel: nunca mexe nos modelos reais
    repo._q(c, f"DROP TABLE IF EXISTS {tabela}")
    repo._q(c, f"CREATE TABLE {tabela} LIKE {repo.T_MENSAGENS}")
    try:
        repo.salvar_mensagem(c, "A", "texto A", eh_inicial=True, tabela=tabela)
        repo.salvar_mensagem(c, "B", "texto B", eh_inicial=False, tabela=tabela)
        df = repo.mensagens_df(c, tabela)
        assert len(df) == 2 and df[df["EhInicial"]]["Nome"].tolist() == ["A"]
        repo.salvar_mensagem(c, "B", "texto B novo", eh_inicial=True, tabela=tabela)
        df = repo.mensagens_df(c, tabela)
        assert len(df) == 2, "MERGE nao duplica"
        assert df[df["EhInicial"]]["Nome"].tolist() == ["B"], "marcar B como inicial desmarca A"
        assert df[df["Nome"] == "B"].iloc[0]["Texto"] == "texto B novo"
        repo.salvar_mensagem(c, "A", "texto A", ativo=False, tabela=tabela)
        assert bool(repo.mensagens_df(c, tabela).set_index("Nome").loc["A", "Ativo"]) is False
        for nome, texto in (("", "x"), ("   ", "x"), ("C", ""), ("C", "   "), ("x" * 61, "x")):
            try:
                repo.salvar_mensagem(c, nome, texto, tabela=tabela)
            except ValueError:
                continue
            raise AssertionError(f"deveria rejeitar nome={nome!r} texto={texto!r}")
    finally:
        repo._q(c, f"DROP TABLE IF EXISTS {tabela}")


def test_mensagens_reais_para_a_fila():
    """So leitura, com o ciclo real: monta a mensagem de cada cliente da fila e confere consistencia."""
    c = _cliente()
    if not repo.ciclos_iniciados(c):
        print("  (pulado: nenhum ciclo iniciado)")
        return
    fila = repo.fila(c)
    if fila.empty:
        return
    todas = repo.sessoes_clientes(c, fila["CodigoCliente"].astype(int).tolist())
    modelo = "{nome_contato}|{nome_cliente}|{animais}|{mes}|{lista_sessoes}|{total}"
    for _, linha in fila.iterrows():
        s = todas[todas["CodigoCliente"] == linha["CodigoCliente"]]
        assert len(s) == int(linha["QtdSessoes"])
        assert abs(float(sum(s["Valor"])) - float(linha["TotalEmAberto"])) < 0.005, "soma das sessoes = total da fila"
        msg = repo.renderizar_mensagem(modelo, repo.montar_contexto(linha, s))
        assert "{" not in msg, f"marcador nao resolvido: {msg!r}"
        assert repo.brl(linha["TotalEmAberto"]) in msg
        assert msg.count(" - R$ ") == len(s), "uma linha por sessao"
        url, _ = repo.link_whatsapp(linha["TelefoneWhatsapp"] or "0", msg)
        assert url.startswith("https://web.whatsapp.com/send?phone=")
    for m in repo.mensagens_df(c).itertuples():
        assert repo.marcadores_desconhecidos(m.Texto) == [], f"modelo {m.Nome!r} tem marcador desconhecido"


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
