"""App de cobranca FisioVet (Streamlit) - fila de cobranca, contatos, modelos de mensagem e envio por link wa.me.

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


@st.cache_data(ttl=30, show_spinner=False)
def _mensagens(_c):
    return repo.mensagens_df(_c)


@st.cache_data(ttl=30, show_spinner=False)
def _ciclos_foto(_c):
    return repo.ciclos_com_foto(_c)


@st.cache_data(ttl=30, show_spinner=False)
def _historico(_c, mes):
    return repo.historico_df(_c, mes)


@st.cache_data(ttl=30, show_spinner=False)
def _envios_ciclo(_c, mes):
    return repo.envios_do_ciclo(_c, mes)


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
                r = repo.iniciar_novo_ciclo(c, sugerido)
                msg = f"Cobrança de {repo.nome_mes(sugerido)} iniciada."
                if r["ciclo_fechado"]:
                    msg += (f" A foto do ciclo de {repo.nome_mes(r['ciclo_fechado'])} foi gravada "
                            f"({r['fotografados']} cliente(s)): veja na aba Histórico.")
                avisar(msg)
                recarregar()
            st.caption("Antes de começar, o app grava a **foto do ciclo atual** (quem foi cobrado, NF, quem quitou) no "
                       "histórico. Depois o corte anda para o fim do novo mês, e o “não cobrar” e a NF do ciclo anterior "
                       "expiram. Quem ainda deve meses antigos continua na fila.")
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

    painel_envio(c)

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

    f1, f2, f3, f4 = st.columns([3, 1, 1, 1])
    padrao = ["A_COBRAR", "COBRADO", "NAO_COBRAR_NO_CICLO"]
    mostrar_inc = f2.toggle("Mostrar incobráveis", value=False)
    so_nf = f3.toggle("Só NF pendente", value=False)
    dias_min = int(f4.number_input("Cobrados há ≥ N dias", min_value=0, value=0, step=1,
                                   help="0 = sem filtro. Ex.: 7 mostra só quem foi cobrado há 7 dias ou mais e ainda deve."))
    escolhidos = f1.multiselect("Estados", repo.ESTADOS, default=padrao + (["INCOBRAVEL"] if mostrar_inc else []),
                                format_func=lambda e: ROTULO_ESTADO[e], key=f"estados_{mostrar_inc}")
    visiveis = fila[fila["EstadoFila"].isin(escolhidos)]
    if so_nf:
        visiveis = visiveis[visiveis["NFStatus"] == "PENDENTE"]
    if dias_min > 0:
        visiveis = visiveis[visiveis["DiasDesdeUltimaCobranca"] >= dias_min]
    visiveis = visiveis.reset_index(drop=True)
    if visiveis.empty:
        st.info("Nenhum cliente com esses filtros.")
        return

    evento = st.dataframe(montar_tabela(visiveis), hide_index=True, use_container_width=True,
                          on_select="rerun", selection_mode="multi-row",
                          key=f"fila_{mostrar_inc}_{len(escolhidos)}_{so_nf}_{dias_min}")
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

    secao_envio(c, ciclo, sel, codigos)

    if len(codigos) == 1:
        detalhe_cliente(c, sel.iloc[0])


# ---------------------------------------------------------------- envio de mensagens

def preparar(c, sel, ciclo, nome_msg, texto_msg):
    """Monta o texto e o link de cada cliente selecionado. Quem esta em 'nao cobrar' ou incobravel fica de fora."""
    de_fora = sel["EstadoFila"].isin(["NAO_COBRAR_NO_CICLO", "INCOBRAVEL"])
    alvo = sel[~de_fora]
    sessoes = repo.sessoes_clientes(c, alvo["CodigoCliente"].astype(int).tolist())
    itens = []
    for _, linha in alvo.iterrows():
        s = sessoes[sessoes["CodigoCliente"] == linha["CodigoCliente"]]
        contexto = repo.montar_contexto(linha, s)
        texto = repo.renderizar_mensagem(texto_msg, contexto)
        telefone = linha["TelefoneWhatsapp"] if linha["TelefoneValido"] else None
        url, texto_no_link = repo.link_whatsapp(telefone, texto) if telefone else (None, False)
        itens.append({
            "codigo": int(linha["CodigoCliente"]), "cliente": linha["NomeCliente"], "animais": contexto["animais"],
            "contato": linha["NomeContato"], "telefone": telefone, "texto": texto, "url": url,
            "texto_no_link": texto_no_link, "total": float(linha["TotalEmAberto"]),
            "ja_recebeu": bool(linha["QtdEnviosCiclo"] > 0 and linha["UltimaMensagemCiclo"] == nome_msg),
        })
    return {"mensagem": nome_msg, "ciclo": ciclo, "itens": itens, "ignorados": int(de_fora.sum())}


def secao_envio(c, ciclo, sel, codigos):
    st.divider()
    st.subheader("Enviar mensagem")
    ativas = _mensagens(c)
    ativas = ativas[ativas["Ativo"]]
    if ativas.empty:
        st.warning("Nenhuma mensagem ativa. Crie ou ative uma na aba Mensagens.")
        return
    nomes = ativas["Nome"].tolist()
    iniciais = ativas[ativas["EhInicial"]]["Nome"].tolist()
    ninguem_cobrado = bool((sel["QtdEnviosCiclo"] == 0).all())
    sugerida = iniciais[0] if (ninguem_cobrado and iniciais) else next((n for n in nomes if n not in iniciais), nomes[0])
    escolha = st.selectbox("Mensagem", nomes, index=nomes.index(sugerida), key="msg_" + "_".join(map(str, codigos)),
                           help="Sugestão: a inicial para quem ainda não foi cobrado neste ciclo; para quem já foi, outra.")
    if st.button("Preparar mensagens dos selecionados", type="primary"):
        texto = ativas[ativas["Nome"] == escolha].iloc[0]["Texto"]
        st.session_state["preparados"] = preparar(c, sel, ciclo, escolha, texto)
        st.session_state["enviados"] = set()
        st.rerun()


def painel_envio(c):
    prep = st.session_state.get("preparados")
    if not prep:
        return
    enviados = st.session_state.setdefault("enviados", set())
    with st.container(border=True):
        topo, fechar = st.columns([5, 1])
        topo.subheader(f"Mensagens prontas — “{prep['mensagem']}” ({len(prep['itens'])})")
        if fechar.button("Fechar painel"):
            st.session_state.pop("preparados", None)
            st.session_state.pop("enviados", None)
            st.rerun()
        st.caption("1) Clique em **Abrir WhatsApp Web** (abre a conversa no navegador, com o texto pronto; a conta precisa estar "
                   "logada nesse navegador) e envie lá. 2) Volte e clique em **Marcar como enviado**. "
                   "Abrir o link não registra nada: só o botão registra.")
        if prep["ignorados"]:
            st.caption(f"{prep['ignorados']} cliente(s) ficaram de fora (“não cobrar” neste ciclo ou incobrável).")
        for item in prep["itens"]:
            cod = item["codigo"]
            with st.container(border=True):
                fone = repo.formatar_telefone(item["telefone"]) or "sem telefone válido"
                st.markdown(f"**{item['cliente']}** · {item['animais']} · {item['contato']} · {fone} · {repo.brl(item['total'])}")
                if item["ja_recebeu"]:
                    st.warning("Este cliente já recebeu esta mensagem neste ciclo.")
                st.code(item["texto"], language=None)
                b1, b2, b3 = st.columns([1, 1, 2])
                if item["url"]:
                    b1.link_button("Abrir WhatsApp Web", item["url"])
                    if not item["texto_no_link"]:
                        b3.caption("Mensagem longa: o link abre a conversa sem texto. Copie o texto acima e cole.")
                else:
                    b1.warning("Sem telefone válido: edite o contato.")
                if cod in enviados:
                    b2.success("Enviado ✔")
                    if b3.button("Desfazer", key=f"des_{cod}"):
                        repo.desfazer_ultimo_envio(c, cod, prep["ciclo"])
                        enviados.discard(cod)
                        recarregar()
                elif b2.button("Marcar como enviado", key=f"env_{cod}", disabled=not item["url"]):
                    repo.registrar_envio(c, cod, prep["ciclo"], prep["mensagem"], item["contato"],
                                         item["telefone"], item["total"], item["texto"])
                    enviados.add(cod)
                    recarregar()


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


# ---------------------------------------------------------------- aba: mensagens

DESCRICAO_MARCADORES = {
    "nome_contato": "como chamar a pessoa (aba Contatos), ex.: Maria",
    "nome_cliente": "nome do tutor no cadastro",
    "animais": "animais com sessão em aberto, ex.: Mel e Thor",
    "mes": "mês do ciclo, ex.: agosto de 2026",
    "lista_sessoes": "uma linha por sessão (agrupada por mês, com subtotal, se houver mais de um mês)",
    "total": "valor total em aberto, ex.: R$ 1.234,50",
}


def exemplo_previa(c, ciclo):
    """(linha, sessoes) para a previa: o primeiro cliente REAL da fila; se nao houver, um exemplo inventado."""
    if ciclo:
        fila = _fila(c)
        if not fila.empty:
            linha = fila.iloc[0]
            return linha, _sessoes(c, int(linha["CodigoCliente"]))
    mes = repo.mes_sugerido()
    linha = {"NomeContato": "Maria", "NomeCliente": "Maria da Silva", "MesCiclo": mes, "TotalEmAberto": 270}
    sessoes = pd.DataFrame(
        [(mes.replace(day=5), "Thor", "Fisioterapia", 120, False), (mes.replace(day=12), "Mel", "Acupuntura", 150, False)],
        columns=["DataSessao", "NomeAnimal", "ProdutoServico", "Valor", "Parcial"])
    return linha, sessoes


def aba_mensagens(c, ciclo):
    df = _mensagens(c)
    st.markdown("**Marcadores** que você pode usar no texto: "
                + " · ".join(f"`{{{m}}}`" for m in repo.MARCADORES))
    with st.expander("O que cada marcador vira"):
        for m in repo.MARCADORES:
            st.markdown(f"- `{{{m}}}` — {DESCRICAO_MARCADORES[m]}")

    NOVA = "➕ Nova mensagem"
    escolha = st.selectbox("Mensagem", [NOVA] + df["Nome"].tolist(), key="msg_editar")
    atual = None if escolha == NOVA else df[df["Nome"] == escolha].iloc[0]

    nome = st.text_input("Nome", value="" if atual is None else atual["Nome"], disabled=atual is not None,
                         key=f"m_nome_{escolha}",
                         help="O nome não muda depois de criado (o histórico de envios guarda esse nome). "
                              "Para trocar, crie outra mensagem e desative esta.")
    texto = st.text_area("Texto", value="" if atual is None else atual["Texto"], height=300, key=f"m_texto_{escolha}",
                         help="Use *texto* para negrito no WhatsApp. Aperte Ctrl+Enter (ou clique fora) para atualizar a prévia.")
    col1, col2 = st.columns(2)
    ativa = col1.checkbox("Ativa (aparece na hora de enviar)", value=True if atual is None else bool(atual["Ativo"]),
                          key=f"m_ativa_{escolha}")
    inicial = col2.checkbox("É a mensagem inicial (padrão para quem ainda não foi cobrado)",
                            value=False if atual is None else bool(atual["EhInicial"]), key=f"m_ini_{escolha}")

    desconhecidos = repo.marcadores_desconhecidos(texto)
    if desconhecidos:
        st.warning("Marcador(es) que o app não conhece: " + ", ".join(f"`{{{m}}}`" for m in desconhecidos)
                   + ". Eles ficariam escritos assim na mensagem. Confira a digitação.")

    linha, sessoes = exemplo_previa(c, ciclo)
    st.markdown("**Prévia**")
    st.code(repo.renderizar_mensagem(texto, repo.montar_contexto(linha, sessoes)), language=None)
    st.caption("Prévia com o primeiro cliente da fila (ou um exemplo inventado, se a fila estiver vazia).")

    if st.button("Salvar mensagem", type="primary"):
        try:
            repo.salvar_mensagem(c, nome if atual is None else atual["Nome"], texto, ativa, inicial)
        except ValueError as erro:
            st.error(str(erro))
        else:
            avisar(f"Mensagem “{(nome if atual is None else atual['Nome']).strip()}” salva.")
            recarregar()


# ---------------------------------------------------------------- aba: historico

def fmt_dt(ts) -> str:
    if ts is None or pd.isna(ts):
        return ""
    return pd.Timestamp(ts).tz_convert(repo.TZ).strftime("%d/%m %H:%M")


def nf_historico(linha) -> str:
    nf = linha["NotaFiscal"]
    if not isinstance(nf, str) or not nf:
        return ""
    return f"{'com CPF' if nf == 'COM_CPF' else 'sem CPF'} — {'emitida' if linha['NFEmitida'] else 'NÃO emitida'}"


def aba_historico(c, ciclo):
    todos = sorted(set(_ciclos(c)), reverse=True)
    if not todos:
        st.info("Nenhum ciclo iniciado ainda.")
        return
    fotos = _ciclos_foto(c)

    def rotulo(mes):
        return repo.nome_mes(mes) + (" — em andamento" if mes == ciclo else "") + (" — encerrado" if mes in fotos else "")

    mes = st.selectbox("Ciclo", todos, format_func=rotulo, key="hist_ciclo")

    if mes in fotos:
        h = _historico(c, mes)
        aberto = h[h["SituacaoFinal"] == "EM_ABERTO"]
        precisam_nf = h[h["NotaFiscal"].notna()]
        cols = st.columns(5)
        cols[0].metric("Clientes no ciclo", len(h))
        cols[1].metric("Cobrados", int((h["QtdEnvios"] > 0).sum()), f"{int(h['QtdEnvios'].sum())} envio(s)", delta_color="off")
        cols[2].metric("Ainda deviam ao encerrar", len(aberto), repo.brl(aberto["EmAbertoNoFechamento"].sum()), delta_color="off")
        cols[3].metric("Quitaram", int((h["SituacaoFinal"] == "QUITADO").sum()))
        cols[4].metric("NF emitida", f"{int(precisam_nf['NFEmitida'].sum())} de {len(precisam_nf)}")
        tabela = pd.DataFrame({
            "Cliente": h["NomeCliente"],
            "Envios": h["QtdEnvios"],
            "1ª cobrança": h["PrimeiraCobrancaEm"].map(fmt_dt),
            "Última": h["UltimaCobrancaEm"].map(fmt_dt),
            "Última mensagem": h["UltimaMensagem"].fillna(""),
            "Valor cobrado": h["ValorCobrado"].map(lambda v: repo.brl(v) if pd.notna(v) else ""),
            "Devia ao encerrar": h["EmAbertoNoFechamento"].map(repo.brl),
            "Resultado": h["SituacaoFinal"].map({"QUITADO": "Quitou", "EM_ABERTO": "Ainda devia"}),
            "NF": h.apply(nf_historico, axis=1),
            "Marcações": h.apply(lambda r: ", ".join(x for x, ativo in (("não cobrar no ciclo", r["NaoCobrarNoCiclo"]),
                                                                        ("incobrável", r["Incobravel"])) if ativo), axis=1),
        })
        st.dataframe(tabela, hide_index=True, use_container_width=True)
        st.caption(f"Foto gravada ao iniciar o ciclo seguinte ({fmt_dt(h['FechadoEm'].iloc[0])}). "
                   "Ela não muda depois: é o retrato de como o ciclo terminou.")
    elif mes == ciclo:
        st.info("Este ciclo está em andamento: o estado atual está na aba **Fila**. A foto do ciclo (quem foi cobrado, NF, "
                "quem quitou) é gravada automaticamente quando você iniciar o ciclo seguinte.")
    else:
        st.info("Este ciclo não tem foto (foi encerrado antes do histórico existir). Os envios registrados aparecem abaixo.")

    env = _envios_ciclo(c, mes)
    st.markdown(f"**Envios registrados em {repo.nome_mes(mes)}** — {len(env)}")
    if env.empty:
        st.caption("Nenhum envio registrado neste ciclo.")
        return
    st.dataframe(pd.DataFrame({
        "Quando": env["EnviadoEm"].map(fmt_dt), "Cliente": env["NomeCliente"], "Mensagem": env["MensagemNome"],
        "Valor": env["ValorNoEnvio"].map(repo.brl)}), hide_index=True, use_container_width=True)
    with st.expander("Ver os textos enviados"):
        for _, e in env.iterrows():
            st.markdown(f"**{e['NomeCliente']}** · {e['MensagemNome']} · {fmt_dt(e['EnviadoEm'])}")
            st.code(e["TextoEnviado"], language=None)


# ---------------------------------------------------------------- principal

def main():
    c = cliente()
    ciclo = max(_ciclos(c)) if _ciclos(c) else None

    aviso = st.session_state.pop("aviso", None)
    st.title("💬 Cobrança FisioVet")
    if aviso:
        st.success(aviso)

    barra_lateral(c, ciclo)
    tab_fila, tab_contatos, tab_mensagens, tab_historico = st.tabs(["Fila de cobrança", "Contatos", "Mensagens", "Histórico"])
    with tab_fila:
        aba_fila(c, ciclo)
    with tab_contatos:
        aba_contatos(c)
    with tab_mensagens:
        aba_mensagens(c, ciclo)
    with tab_historico:
        aba_historico(c, ciclo)


main()
