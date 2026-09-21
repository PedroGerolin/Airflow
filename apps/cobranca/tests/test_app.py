"""Testes da tela (app.py) sem navegador, com o AppTest do Streamlit. Funcionam com pytest OU: python tests/test_app.py

Limites do AppTest: nao consegue clicar em linhas de st.dataframe nem editar st.data_editor; essas partes
ficam para conferencia manual. Os testes que iniciam ciclo usam dado REAL do BigQuery e apagam no finally;
se ja existir ciclo real iniciado, sao pulados.
"""
import os
import sys
from datetime import date

import streamlit as st
from streamlit.testing.v1 import AppTest

AQUI = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(AQUI, ".."))
import repo  # noqa: E402


def _app() -> AppTest:
    # o app guarda consultas em cache por 30 s (st.cache_data); sem limpar, um teste enxerga o estado do anterior
    st.cache_data.clear()
    return AppTest.from_file(os.path.join(AQUI, "..", "app.py"), default_timeout=120)


def _ha_ciclo_real(c) -> bool:
    if repo.ciclos_iniciados(c):
        print("  (pulado: ja existe ciclo real iniciado)")
        return True
    return False


def test_app_sem_ciclo():
    c = repo.get_client()
    if _ha_ciclo_real(c):
        return
    at = _app().run()
    assert not at.exception, at.exception
    assert any("Nenhum ciclo iniciado" in w.value for w in at.sidebar.warning)
    assert any("Iniciar cobrança de" in b.label for b in at.sidebar.button)
    assert any("Inicie a cobrança" in i.value for i in at.info), "fila deveria orientar a iniciar o ciclo"
    assert len(at.tabs) == 4


def test_app_inicia_ciclo_e_mostra_fila():
    c = repo.get_client()
    if _ha_ciclo_real(c):
        return
    sugerido = repo.mes_sugerido()
    at = _app().run()
    botao = next(b for b in at.sidebar.button if "Iniciar cobrança" in b.label)
    assert repo.nome_mes(sugerido) in botao.label
    try:
        botao.click().run()
        assert not at.exception, at.exception
        assert any("iniciada" in s.value for s in at.success), "deveria confirmar o inicio do ciclo"
        assert any(repo.nome_mes(sugerido) in s.value for s in at.sidebar.success)
        assert len(at.metric) == 4, "os 4 cartoes de estado"
        assert len(at.dataframe) >= 1, "tabela da fila"
        assert not any("Iniciar cobrança" in b.label for b in at.sidebar.button), "mes iniciado nao oferece iniciar de novo"
    finally:
        repo._q(c, f"DELETE FROM {repo.T_CICLOS} WHERE MesReferencia = @m", m=("DATE", sugerido))


def test_aba_contatos_renderiza():
    at = _app().run()
    assert not at.exception, at.exception
    assert len(at.tabs) == 4


def test_aba_mensagens_previa_e_marcador_desconhecido():
    """So leitura: mostra o modelo, renderiza a previa sem marcador sobrando e avisa de marcador desconhecido."""
    at = _app().run()
    assert not at.exception, at.exception
    at.selectbox(key="msg_editar").select("Inicial").run()
    assert not at.exception, at.exception
    texto = at.text_area(key="m_texto_Inicial")
    assert "{lista_sessoes}" in texto.value and "{animais}" in texto.value
    previas = [c.value for c in at.code if "Bom dia" in c.value]
    assert previas and "{" not in previas[0], "a previa tem que ter todos os marcadores resolvidos"
    assert not any("não conhece" in w.value for w in at.warning), "modelo padrao nao tem marcador desconhecido"
    texto.input("Oi {nome_contato}, chave {pix} e {total}").run()   # nao salva: so digita
    assert not at.exception, at.exception
    assert any("pix" in w.value for w in at.warning), "deveria avisar do marcador {pix}, que nao existe mais"


def test_aba_historico_ciclo_em_andamento():
    """So leitura: com um ciclo real ainda nao encerrado, a aba explica que a foto sai ao iniciar o proximo."""
    c = repo.get_client()
    if not repo.ciclos_iniciados(c) or repo.ciclos_com_foto(c):
        print("  (pulado: precisa de ciclo real iniciado e sem foto)")
        return
    at = _app().run()
    assert not at.exception, at.exception
    assert any("em andamento" in i.value for i in at.info), "deveria explicar que o ciclo esta em andamento"
    assert any("Envios registrados" in m.value for m in at.markdown)


def test_aba_historico_mostra_ciclo_encerrado():
    """Grava um ciclo de mentira (1999-01, cliente -1) ja fotografado e confere que a aba mostra o resumo."""
    c = repo.get_client()
    mes = date(1999, 1, 1)
    try:
        repo.iniciar_ciclo(c, mes)
        repo.salvar_contato(c, -1, "Teste", None, None, revisado=False)
        repo.definir_nota_fiscal(c, -1, "COM_CPF")
        repo.registrar_envio(c, -1, mes, "Inicial", "Maria", "5511999999999", 50.0, "texto que foi enviado")
        assert repo.fechar_ciclo(c, mes) == 1
        at = _app().run()
        at.selectbox(key="hist_ciclo").select(mes).run()
        assert not at.exception, at.exception
        rotulos = [m.label for m in at.metric]
        assert "Ainda deviam ao encerrar" in rotulos and "Quitaram" in rotulos and "NF emitida" in rotulos
        assert any("Foto gravada" in cap.value for cap in at.caption)
        assert any("Envios registrados" in m.value for m in at.markdown)
    finally:
        for tabela in (repo.T_HISTORICO, repo.T_ENVIOS, repo.T_CONTATOS):
            repo._q(c, f"DELETE FROM {tabela} WHERE CodigoCliente = @k", k=("INT64", -1))
        repo._q(c, f"DELETE FROM {repo.T_CICLOS} WHERE MesReferencia = @m", m=("DATE", mes))


def test_painel_de_envio_marca_e_desfaz():
    """Injeta mensagens prontas de um cliente de mentira (-1) e testa 'Marcar como enviado' e 'Desfazer'."""
    c = repo.get_client()
    if not repo.ciclos_iniciados(c):
        print("  (pulado: precisa de um ciclo real iniciado para a fila aparecer)")
        return
    item = {"codigo": -1, "cliente": "Cliente de Teste", "animais": "Mel", "contato": "Maria", "telefone": "5511999999999",
            "texto": "Bom dia, Maria!", "url": "https://web.whatsapp.com/send?phone=5511999999999&text=Bom%20dia", "texto_no_link": True,
            "total": 10.5, "ja_recebeu": False}
    sem_fone = dict(item, codigo=-2, telefone=None, url=None, texto_no_link=False)
    at = _app()
    at.session_state["preparados"] = {"mensagem": "Inicial", "ciclo": date(1999, 1, 1), "ignorados": 1, "itens": [item, sem_fone]}
    at.session_state["enviados"] = set()
    n = lambda: list(repo._q(c, f"SELECT COUNT(*) AS n FROM {repo.T_ENVIOS} WHERE CodigoCliente = -1"))[0]["n"]
    try:
        at.run()
        assert not at.exception, at.exception
        assert any("Mensagens prontas" in s.value for s in at.subheader)
        assert at.button(key="env_-2").disabled, "sem telefone valido nao pode marcar como enviado"
        assert n() == 0
        at.button(key="env_-1").click().run()
        assert not at.exception, at.exception
        assert n() == 1, "Marcar como enviado grava em envios"
        assert any("Enviado" in s.value for s in at.success)
        at.button(key="des_-1").click().run()
        assert not at.exception, at.exception
        assert n() == 0, "Desfazer apaga o envio"
    finally:
        repo._q(c, f"DELETE FROM {repo.T_ENVIOS} WHERE CodigoCliente = @k", k=("INT64", -1))


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
