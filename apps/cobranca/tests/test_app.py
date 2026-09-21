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
    assert len(at.tabs) == 2


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
    assert len(at.tabs) == 2


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
