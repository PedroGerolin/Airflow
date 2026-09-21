"""App de cobranca FisioVet (Streamlit) - v1: fila de cobranca + cadastro de contatos.

Toda a logica de dados esta em repo.py; aqui fica so a tela. Desenho e regras de negocio: ROADMAP.md
(secao "Cobranca por WhatsApp"), nas anotacoes do usuario. Roda so em localhost (ver docker-compose.yaml).
"""
import pandas as pd
import streamlit as st

import repo

st.set_page_config(page_title="Cobrança FisioVet", page_icon="💬", layout="wide")

ROTULO_ESTADO = {
    "A_COBRAR": "A cobrar",
    "COBRADO": "Cobrado",
    "NAO_COBRAR_NO_CICLO": "Devendo — não cobrar (este ciclo)",
    "INCOBRAVEL": "Incobrável",
}


@st.cache_resource
def cliente():
    return repo.get_client()


@st.cache_data(ttl=30, show_spinner=False)
def _ciclos(_c):
    return repo.ciclos_iniciados(_c)


@st.cache_data(ttl=30, show_spinner=False)
def _fila(_c):
    return repo.fila(_c)


@st.cache_data(ttl=30, show_spinner=False)
def _sessoes(_c, codigo):
    return repo.sessoes_cliente(_c, codigo)


@st.cache_data(ttl=30, show_spinner=False)
def _contatos(_c):
    return repo.contatos_df(_c)


def avisar(texto: str):
    """Guarda uma mensagem para aparecer depois do proximo rerun (st.rerun apaga o que foi mostrado antes)."""
    st.session_state["aviso"] = texto


def recarregar():
    st.cache_data.clear()
    st.rerun()


def estado_legivel(linha) -> str:
    if linha["EstadoFila"] == "COBRADO":
        dias = linha["DiasDesdeUltimaCobranca"]
        msg = linha["UltimaMensagemCiclo"] or "mensagem"
        return f"Cobrado há {int(dias)} dia(s) — {msg}" if pd.notna(dias) else "Cobrado"
    return ROTULO_ESTADO[linha["EstadoFila"]]


def avisos_da_linha(linha) -> str:
    itens = []
    if not linha["TelefoneValido"]:
        itens.append("sem telefone válido")
    if not linha["ContatoRevisado"]:
        itens.append("contato não conferido")
    if linha["TemBaixaParcial"]:
        itens.append("baixa parcial")
    if linha["NotaFiscal"] == "COM_CPF" and not linha["TemCPF"]:
        itens.append("NF com CPF, mas sem CPF no cadastro")
    return ", ".join(itens)


def nf_legivel(linha) -> str:
    """'' (nao precisa) | 'com CPF — PENDENTE' | 'sem CPF — emitida'"""
    nf = linha["NotaFiscal"]
    if not isinstance(nf, str) or not nf:
        return ""
    tipo = "com CPF" if nf == "COM_CPF" else "sem CPF"
    return f"{tipo} — {'emitida' if linha['NFStatus'] == 'EMITIDA' else 'PENDENTE'}"


def montar_tabela(fila: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "Cliente": fila["NomeCliente"],
        "Animais": fila["Animais"].fillna("").str.replace(", ", " / ", regex=False),
        "Como chamar": fila["NomeContato"],
        "WhatsApp": fila["TelefoneWhatsapp"].map(repo.formatar_telefone),
        "Sessões": fila["QtdSessoes"],
        "Total em aberto": fila["TotalEmAberto"].map(repo.brl),
        "Desde": fila["MesMaisAntigo"].map(lambda d: f"{d:%m/%Y}" if pd.notna(d) else ""),
        "Estado": fila.apply(estado_legivel, axis=1),
        "NF": fila.apply(nf_legivel, axis=1),
        "Avisos": fila.apply(avisos_da_linha, axis=1),
    })


# ---------------------------------------------------------------- barra lateral

def barra_lateral(c, ciclo):
    with st.sidebar:
        st.header("Cobrança")
        if ciclo:
            st.success(f"Ciclo atual: **{repo.nome_mes(ciclo)}**")
            st.caption(f"Entram todas as sessões em aberto até {repo.fim_do_mes(ciclo):%d/%m/%Y}. "
                       "Nada do mês corrente.")
        else:
            st.warning("Nenhum ciclo iniciado. A fila fica vazia até você iniciar a cobrança de um mês.")

        sugerido = repo.mes_sugerido()
        if sugerido not in _ciclos(c):
            st.divider()
            st.caption("Novo ciclo")
            if st.button(f"Iniciar cobrança de {repo.nome_mes(sugerido)}", type="primary"):
                repo.iniciar_ciclo(c, sugerido)
                avisar(f"Cobrança de {repo.nome_mes(sugerido)} iniciada.")
                recarregar()
            st.caption("O corte anda para o fim desse mês, e o “não cobrar” do ciclo anterior expira. "
                       "Quem ainda deve meses antigos continua na fila.")
        st.divider()
        if st.button("Atualizar dados"):
            recarregar()


# ---------------------------------------------------------------- aba: fila

def aba_fila(c, ciclo):
    if not ciclo:
        st.info("Inicie a cobrança de um mês no menu à esquerda para ver a fila.")
        return

    fila = _fila(c)
    if fila.empty:
        st.success("Ninguém deve nada até o fim do ciclo atual. 🎉")
        return

    st.caption(f"Vendas carregadas até {fila['DadosAteData'].iloc[0]:%d/%m/%Y}. "
               "Uma baixa dada agora no sistema só aparece aqui depois da próxima atualização diária.")

    resumo = repo.resumo_por_estado(fila)
    colunas = st.columns(4)
    for col, estado in zip(colunas, repo.ESTADOS):
        qtd, total = resumo[estado]
        col.metric(ROTULO_ESTADO[estado], f"{qtd} cliente(s)", repo.brl(total), delta_color="off")

    com_nf = int(fila["NotaFiscal"].notna().sum())
    if com_nf:
        pendentes = int((fila["NFStatus"] == "PENDENTE").sum())
        st.caption(f"Nota fiscal: **{pendentes} pendente(s)** de {com_nf} cliente(s) que precisam de NF neste ciclo.")

    f1, f2, f3 = st.columns([3, 1, 1])
    padrao = ["A_COBRAR", "COBRADO", "NAO_COBRAR_NO_CICLO"]
    mostrar_inc = f2.toggle("Mostrar incobráveis", value=False)
    so_nf = f3.toggle("Só NF pendente", value=False)
    escolhidos = f1.multiselect("Estados", repo.ESTADOS, default=padrao + (["INCOBRAVEL"] if mostrar_inc else []),
                                format_func=lambda e: ROTULO_ESTADO[e], key=f"estados_{mostrar_inc}")
    visiveis = fila[fila["EstadoFila"].isin(escolhidos)]
    if so_nf:
        visiveis = visiveis[visiveis["NFStatus"] == "PENDENTE"]
    visiveis = visiveis.reset_index(drop=True)
    if visiveis.empty:
        st.info("Nenhum cliente com esses filtros.")
        return

    evento = st.dataframe(montar_tabela(visiveis), hide_index=True, use_container_width=True,
                          on_select="rerun", selection_mode="multi-row",
                          key=f"fila_{mostrar_inc}_{len(escolhidos)}_{so_nf}")
    linhas = evento.selection.rows
    if not linhas:
        st.caption("Selecione uma ou mais linhas para agir sobre elas.")
        return

    sel = visiveis.iloc[linhas]
    codigos = sel["CodigoCliente"].astype(int).tolist()
    st.subheader(f"{len(codigos)} selecionado(s) — {repo.brl(sel['TotalEmAberto'].sum())}")

    b1, b2, b3, b4 = st.columns(4)
    if b1.button("Não cobrar neste ciclo", use_container_width=True):
        for cod in codigos:
            repo.definir_nao_cobrar(c, cod, ciclo)
        avisar(f"{len(codigos)} cliente(s) marcados como “devendo, não cobrar” em {repo.nome_mes(ciclo)}.")
        recarregar()
    if b2.button("Voltar a cobrar", use_container_width=True, help="Desfaz o “não cobrar” deste ciclo"):
        for cod in codigos:
            repo.definir_nao_cobrar(c, cod, None)
        avisar(f"{len(codigos)} cliente(s) voltaram para a fila de cobrança.")
        recarregar()
    with b3:
        confirmo = st.checkbox("Confirmo: incobrável sai da fila em todos os ciclos", key="conf_inc")
        if st.button("Marcar como incobrável", disabled=not confirmo, use_container_width=True):
            for cod in codigos:
                repo.definir_situacao(c, cod, "INCOBRAVEL")
            avisar(f"{len(codigos)} cliente(s) marcados como incobráveis.")
            recarregar()
    if b4.button("Reativar (deixa de ser incobrável)", use_container_width=True):
        for cod in codigos:
            repo.definir_situacao(c, cod, "ATIVO")
        avisar(f"{len(codigos)} cliente(s) reativados.")
        recarregar()

    n1, n2, _, _ = st.columns(4)
    if n1.button("Marcar NF emitida", use_container_width=True,
                 help="Vale só para quem tem NF configurada. Expira sozinha quando você iniciar o mês seguinte."):
        feitos = sum(repo.definir_nf_emitida(c, cod, ciclo) for cod in codigos)
        ignorados = len(codigos) - feitos
        avisar(f"NF marcada como emitida em {feitos} cliente(s)."
               + (f" {ignorados} ignorado(s): sem NF configurada." if ignorados else ""))
        recarregar()
    if n2.button("Desmarcar NF emitida", use_container_width=True):
        for cod in codigos:
            repo.definir_nf_emitida(c, cod, None)
        avisar(f"NF desmarcada em {len(codigos)} cliente(s).")
        recarregar()

    if len(codigos) == 1:
        detalhe_cliente(c, sel.iloc[0])


def detalhe_cliente(c, linha):
    codigo = int(linha["CodigoCliente"])
    st.divider()
    st.subheader(linha["NomeCliente"])
    esq, dir_ = st.columns([3, 2])

    with esq:
        st.markdown("**Sessões em aberto**")
        s = _sessoes(c, codigo).copy()
        s["Parcial"] = s["Parcial"].map(lambda p: "sim" if p else "")
        s["Valor"] = s["Valor"].map(repo.brl)
        s["DataSessao"] = s["DataSessao"].map(lambda d: f"{d:%d/%m/%Y}")
        s.columns = ["Data", "Animal", "Serviço", "Valor", "Baixa parcial"]
        st.dataframe(s, hide_index=True, use_container_width=True)
        if linha["OrigemContato"] == "CADASTRO":
            st.caption("O contato abaixo ainda vem do cadastro do sistema. Salve para gravá-lo no app.")

    with dir_:
        st.markdown("**Contato de cobrança**")
        with st.form(f"contato_{codigo}"):
            nome = st.text_input("Como chamar na mensagem", value=linha["NomeContato"] or "",
                                 help="Só o primeiro nome, ou quem recebe a cobrança, como você diria no "
                                      "cumprimento (ex.: Maria, Sr. Carlos). Vira {nome_contato} na mensagem.")
            tel = st.text_input("WhatsApp (DDD + número)", value=repo.formatar_telefone(linha["TelefoneWhatsapp"]))
            obs = st.text_input("Observação", value=linha["Observacao"] or "")
            opcoes_nf = [repo.NF_SEM] + list(repo.NF_ROTULOS.values())
            nf_atual = repo.rotulo_nf(linha["NotaFiscal"]) or repo.NF_SEM
            nf_escolhida = st.selectbox("Nota fiscal", opcoes_nf, index=opcoes_nf.index(nf_atual),
                                        help="Deixe “sem NF” para quem não precisa de nota.")
            rev = st.checkbox("Marcar como conferido", value=True)
            if st.form_submit_button("Salvar contato", type="primary"):
                try:
                    repo.salvar_contato(c, codigo, nome, tel, obs, rev)
                    nf_nova = repo.nf_do_rotulo(nf_escolhida)
                    if nf_nova != (linha["NotaFiscal"] if isinstance(linha["NotaFiscal"], str) else None):
                        repo.definir_nota_fiscal(c, codigo, nf_nova)
                except ValueError as erro:
                    st.error(str(erro))
                else:
                    avisar(f"Contato de {linha['NomeCliente']} salvo.")
                    recarregar()


# ---------------------------------------------------------------- aba: contatos

def aba_contatos(c):
    df = _contatos(c)
    if df.empty:
        st.info("Nenhum contato cadastrado ainda.")
        return

    so_pendentes = st.toggle("Só os não conferidos", value=False)
    vis = df[~df["Revisado"]] if so_pendentes else df
    vis = vis.reset_index(drop=True)
    st.caption(f"{len(vis)} de {len(df)} contato(s). Edite direto na tabela e clique em Salvar. "
               "“Como chamar” é o nome usado no cumprimento da mensagem (só o primeiro nome, ou quem recebe). "
               "Telefone: DDD + número. Situação INCOBRAVEL tira o cliente da fila em todos os ciclos.")

    base = pd.DataFrame({
        "CodigoCliente": vis["CodigoCliente"],
        "Cliente": vis["NomeCliente"],
        "Animais": vis["Animais"].fillna(""),
        "Como chamar": vis["NomeContato"],
        "WhatsApp": vis["TelefoneWhatsapp"].map(repo.formatar_telefone),
        "Situacao": vis["Situacao"],
        "Nota fiscal": vis["NotaFiscal"].map(repo.rotulo_nf),
        "Observacao": vis["Observacao"],
        "Conferido": vis["Revisado"],
    })
    versao = st.session_state.setdefault("versao_editor", 0)
    chave = f"editor_contatos_{versao}_{so_pendentes}"
    st.data_editor(
        base, key=chave, hide_index=True, use_container_width=True, num_rows="fixed",
        disabled=["CodigoCliente", "Cliente", "Animais"],
        column_config={
            "CodigoCliente": st.column_config.NumberColumn("Código", format="%d"),
            "Animais": st.column_config.TextColumn("Animais", help="Todos os animais do cliente. † = falecido.",
                                                   width="medium"),
            "Situacao": st.column_config.SelectboxColumn("Situação", options=list(repo.SITUACOES), required=True),
            "Nota fiscal": st.column_config.SelectboxColumn(
                "Nota fiscal", options=[repo.NF_SEM] + list(repo.NF_ROTULOS.values()),
                help="Em branco = não precisa de NF. Escolha “— sem NF —” para desfazer."),
            "Observacao": "Observação",
            "Conferido": st.column_config.CheckboxColumn("Conferido"),
        },
    )

    if st.button("Salvar alterações", type="primary"):
        editadas = st.session_state.get(chave, {}).get("edited_rows", {})
        if not editadas:
            st.info("Nada foi alterado.")
            return
        salvos, erros = 0, []
        for idx, mudancas in editadas.items():
            linha = base.iloc[int(idx)].copy()
            for coluna, valor in mudancas.items():
                linha[coluna] = valor
            try:
                repo.salvar_contato(c, int(linha["CodigoCliente"]), linha["Como chamar"], linha["WhatsApp"],
                                    linha["Observacao"], bool(linha["Conferido"]))
                if "Situacao" in mudancas:
                    repo.definir_situacao(c, int(linha["CodigoCliente"]), linha["Situacao"])
                if "Nota fiscal" in mudancas:
                    repo.definir_nota_fiscal(c, int(linha["CodigoCliente"]), repo.nf_do_rotulo(linha["Nota fiscal"]))
                salvos += 1
            except ValueError as erro:
                erros.append(f"{linha['Cliente']}: {erro}")
        for e in erros:
            st.error(e)
        if salvos:
            st.session_state["versao_editor"] = versao + 1
            avisar(f"{salvos} contato(s) salvo(s)." + (f" {len(erros)} com erro." if erros else ""))
            recarregar()


# ---------------------------------------------------------------- principal

def main():
    c = cliente()
    ciclo = max(_ciclos(c)) if _ciclos(c) else None

    aviso = st.session_state.pop("aviso", None)
    st.title("💬 Cobrança FisioVet")
    if aviso:
        st.success(aviso)

    barra_lateral(c, ciclo)
    tab_fila, tab_contatos = st.tabs(["Fila de cobrança", "Contatos"])
    with tab_fila:
        aba_fila(c, ciclo)
    with tab_contatos:
        aba_contatos(c)


main()
