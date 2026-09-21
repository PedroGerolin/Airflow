"""Camada de dados do app de cobranca. Toda a conversa com o BigQuery fica aqui; o app.py so chama estas funcoes.

Regras do arquivo:
- So queries/DML (INSERT, MERGE, UPDATE). NUNCA insert_rows_json (streaming): linhas em streaming buffer ficam
  ~90 minutos sem aceitar UPDATE/DELETE.
- Valores de usuario SEMPRE como parametros (@nome), nunca dentro de f-string. As f-strings daqui so injetam
  nomes de tabela, que vem das constantes abaixo.
- As chaves (CodigoCliente, MesReferencia...) sao logicas: o BigQuery nao impede duplicata, entao quem grava
  usa MERGE / NOT EXISTS.
"""
import os
import re
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal
from urllib.parse import quote
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "gerolingcp")
T_CONTATOS = f"`{PROJECT}.FisioVet_App.contatos`"
T_MENSAGENS = f"`{PROJECT}.FisioVet_App.mensagens`"
T_ENVIOS = f"`{PROJECT}.FisioVet_App.envios`"
T_HISTORICO = f"`{PROJECT}.FisioVet_App.ciclos_historico`"
T_CICLOS = f"`{PROJECT}.FisioVet_App.ciclos`"
V_PENDENCIAS = f"`{PROJECT}.FisioVet_Analytics.cobranca_pendencias`"
V_SESSOES = f"`{PROJECT}.FisioVet_Analytics.cobranca_sessoes`"
T_CLIENTS = f"`{PROJECT}.FisioVet.clients`"
T_ANIMALS = f"`{PROJECT}.FisioVet.animals`"

TZ = ZoneInfo("America/Sao_Paulo")
SITUACOES = ("ATIVO", "INCOBRAVEL")
ESTADOS = ("A_COBRAR", "COBRADO", "NAO_COBRAR_NO_CICLO", "INCOBRAVEL")
NF_ROTULOS = {"COM_CPF": "Fazer NF com CPF", "SEM_CPF": "Fazer NF sem CPF"}
NF_SEM = "— sem NF —"  # opcao da lista para desfazer; no banco vira NULL (em branco)
MESES = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
         "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"]

# Primeiro celular do texto livre do cadastro do Simples Vet, em digitos com DDI (mesma regra do seed e da view).
_SQL_TEL_CADASTRO = r"CONCAT('55', REGEXP_REPLACE(REGEXP_EXTRACT(c.Telefone, r'(\(\d{2}\)\s*9\d{4}-?\d{4})'), r'\D', ''))"


# ---------- utilidades puras (sem BigQuery) ----------

def hoje() -> date:
    return datetime.now(TZ).date()


def mes_sugerido(ref: date | None = None) -> date:
    """Dia 1 do mes anterior: em setembro se cobra agosto."""
    ref = ref or hoje()
    primeiro = ref.replace(day=1)
    return (primeiro.replace(day=1) - timedelta(days=1)).replace(day=1)


def fim_do_mes(mes: date) -> date:
    proximo = (mes.replace(day=1) + timedelta(days=32)).replace(day=1)
    return proximo - timedelta(days=1)


def nome_mes(mes: date) -> str:
    return f"{MESES[mes.month - 1]}/{mes.year}"


def normalizar_telefone(texto) -> str | None:
    """Devolve so digitos com DDI (55 + DDD + numero) ou None se vazio. Levanta ValueError se invalido."""
    if texto is None:
        return None
    digitos = re.sub(r"\D", "", str(texto))
    if not digitos:
        return None
    if digitos.startswith("55") and len(digitos) in (12, 13):
        tel = digitos
    elif len(digitos) in (10, 11):
        tel = "55" + digitos
    else:
        raise ValueError("Telefone inválido: use DDD + número, por exemplo (11) 99999-9999.")
    return tel


def formatar_telefone(tel) -> str:
    """5511999999999 -> (11) 99999-9999 (para exibir e editar)."""
    if not tel or not isinstance(tel, str) or not tel.startswith("55") or len(tel) not in (12, 13):
        return tel or ""
    ddd, resto = tel[2:4], tel[4:]
    corte = len(resto) - 4
    return f"({ddd}) {resto[:corte]}-{resto[corte:]}"


def brl(valor) -> str:
    """1234.5 -> R$ 1.234,50"""
    if valor is None or pd.isna(valor):
        return "R$ 0,00"
    return "R$ " + f"{float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def rotulo_nf(valor) -> str | None:
    """COM_CPF -> 'Fazer NF com CPF'; vazio/None -> None (em branco na tela)."""
    return NF_ROTULOS.get(valor) if isinstance(valor, str) else None


def nf_do_rotulo(rotulo) -> str | None:
    """Volta do rotulo da tela para o valor do banco. '— sem NF —', vazio ou None -> None."""
    if rotulo is None or rotulo == "" or rotulo == NF_SEM or (isinstance(rotulo, float) and pd.isna(rotulo)):
        return None
    for valor, texto in NF_ROTULOS.items():
        if texto == rotulo:
            return valor
    raise ValueError(f"Opção de nota fiscal inválida: {rotulo}")


def resumo_por_estado(fila: pd.DataFrame) -> dict:
    """{estado: (quantidade de clientes, total em aberto)} para os 4 estados (zeros incluidos)."""
    saida = {}
    for estado in ESTADOS:
        parte = fila[fila["EstadoFila"] == estado] if not fila.empty else fila
        total = float(parte["TotalEmAberto"].sum()) if not parte.empty else 0.0
        saida[estado] = (len(parte), total)
    return saida


# ---------- mensagem (puro, sem BigQuery) ----------

MARCADORES = ("nome_contato", "nome_cliente", "animais", "mes", "lista_sessoes", "total")
# URL do wa.me com texto muito longo pode falhar; acima disso o link vai sem texto e o app mostra o texto para copiar.
LIMITE_URL_WHATSAPP = 1800


def mes_extenso(mes: date) -> str:
    """2026-08-01 -> 'agosto de 2026' (para o marcador {mes})."""
    return f"{MESES[mes.month - 1].lower()} de {mes.year}"


def juntar_lista(itens) -> str:
    """['Mel'] -> 'Mel'; ['Mel','Thor'] -> 'Mel e Thor'; ['A','B','C'] -> 'A, B e C'."""
    itens = [str(i) for i in itens if i is not None and str(i).strip()]
    if len(itens) <= 1:
        return "".join(itens)
    return ", ".join(itens[:-1]) + " e " + itens[-1]


def marcadores_desconhecidos(texto: str) -> list[str]:
    """Marcadores {assim} que o app nao conhece (erro de digitacao, ou o antigo {pix})."""
    vistos = []
    for m in re.findall(r"\{(\w+)\}", texto or ""):
        if m not in MARCADORES and m not in vistos:
            vistos.append(m)
    return vistos


def formatar_sessoes(sessoes: pd.DataFrame) -> str:
    """Lista para o marcador {lista_sessoes}: 'dd/mm - Animal - Servico - R$ x' (linhas em ordem de data).
    Se ha mais de um mes, agrupa com titulo *Mes/AAAA* e subtotal; com um mes so, vai a lista direta."""
    if sessoes.empty:
        return ""
    s = sessoes.sort_values("DataSessao", kind="stable")
    meses = sorted({d.replace(day=1) for d in s["DataSessao"]})
    blocos = []
    for mes in meses:
        parte = s[s["DataSessao"].map(lambda d, m=mes: d.replace(day=1) == m)]
        linhas = []
        for _, r in parte.iterrows():
            animal = f"{r['NomeAnimal']} - " if isinstance(r["NomeAnimal"], str) and r["NomeAnimal"].strip() else ""
            servico = r["ProdutoServico"] if isinstance(r["ProdutoServico"], str) else ""
            parcial = " (baixa parcial)" if bool(r["Parcial"]) else ""
            linhas.append(f"{r['DataSessao']:%d/%m} - {animal}{servico} - {brl(r['Valor'])}{parcial}")
        if len(meses) > 1:
            linhas = [f"*{nome_mes(mes)}*"] + linhas + [f"Subtotal: {brl(sum(parte['Valor']))}"]
        blocos.append("\n".join(linhas))
    return "\n\n".join(blocos)


def montar_contexto(linha, sessoes: pd.DataFrame) -> dict:
    """Valores dos marcadores para um cliente da fila (linha de cobranca_pendencias) e suas sessoes em aberto."""
    animais = sorted({str(a) for a in sessoes["NomeAnimal"].dropna() if str(a).strip()})
    return {
        "nome_contato": linha["NomeContato"] or "",
        "nome_cliente": linha["NomeCliente"] or "",
        "animais": juntar_lista(animais),
        "mes": mes_extenso(linha["MesCiclo"]),
        "lista_sessoes": formatar_sessoes(sessoes),
        "total": brl(linha["TotalEmAberto"]),
    }


def renderizar_mensagem(texto: str, contexto: dict) -> str:
    """Troca cada {marcador} conhecido pelo valor. Numa passada so (valor com chaves nao e reprocessado);
    marcador desconhecido fica como esta, para aparecer na conferencia."""
    return re.sub(r"\{(\w+)\}", lambda m: str(contexto[m.group(1)]) if m.group(1) in contexto else m.group(0), texto)


def link_whatsapp(telefone: str, texto: str | None = None) -> tuple[str, bool]:
    """(url, texto_no_link). Abre a conversa direto no WHATSAPP WEB (web.whatsapp.com/send) com o texto pronto.
    O wa.me tentava abrir primeiro o app de computador e perguntava qual usar; o usuario usa a conta Business
    no navegador. Se a URL passar do limite, devolve o link SEM texto (False) e o app mostra o texto para copiar."""
    base = f"https://web.whatsapp.com/send?phone={telefone}"
    if not texto:
        return base, False
    url = f"{base}&text={quote(texto, safe='')}"
    if len(url) > LIMITE_URL_WHATSAPP:
        return base, False
    return url, True


# ---------- BigQuery ----------

def get_client() -> bigquery.Client:
    """Usa GOOGLE_APPLICATION_CREDENTIALS (a chave da SA cobranca-app, montada no container)."""
    return bigquery.Client(project=PROJECT)


def _q(client, sql: str, **params):
    """Roda a query com parametros nomeados: nome=("TIPO", valor)."""
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, t, v) for k, (t, v) in params.items()]
    )
    return client.query(sql, job_config=cfg).result()


def _dml(client, sql: str, **params) -> int:
    """Como _q, mas devolve quantas linhas o INSERT/UPDATE/DELETE afetou."""
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, t, v) for k, (t, v) in params.items()]
    )
    job = client.query(sql, job_config=cfg)
    job.result()
    return job.num_dml_affected_rows or 0


def ciclo_atual(client) -> date | None:
    linhas = list(_q(client, f"SELECT MAX(MesReferencia) AS ciclo FROM {T_CICLOS}"))
    return linhas[0]["ciclo"]


def ciclos_iniciados(client) -> list[date]:
    return [r["MesReferencia"] for r in _q(client, f"SELECT MesReferencia FROM {T_CICLOS} ORDER BY MesReferencia")]


def iniciar_ciclo(client, mes: date) -> bool:
    """Inicia a cobranca de um mes (dia 1). Idempotente: devolve False se o mes ja estava iniciado."""
    if mes.day != 1:
        raise ValueError("MesReferencia deve ser o dia 1 do mês.")
    job = client.query(
        f"""INSERT INTO {T_CICLOS} (MesReferencia, IniciadoEm)
            SELECT @mes, CURRENT_TIMESTAMP()
            FROM (SELECT 1)
            WHERE NOT EXISTS (SELECT 1 FROM {T_CICLOS} WHERE MesReferencia = @mes)""",
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("mes", "DATE", mes)]),
    )
    job.result()
    return (job.num_dml_affected_rows or 0) > 0


def fechar_ciclo(client, mes: date) -> int:
    """Grava a 'foto' do ciclo em ciclos_historico (uma linha por cliente que devia, foi cobrado ou teve NF/pausa
    neste ciclo) e marca ciclos.FechadoEm. Idempotente: quem ja tem foto nao duplica. Devolve quantas linhas gravou.
    O estado 'em aberto no fechamento' vem da view da fila, que so vale para o ciclo MAIS RECENTE; para um ciclo
    antigo essa parte fica vazia (por isso a foto e tirada ao iniciar o proximo, antes de qualquer mudanca)."""
    n = _dml(client, f"""
        INSERT INTO {T_HISTORICO}
            (MesReferencia, CodigoCliente, NomeCliente, QtdEnvios, PrimeiraCobrancaEm, UltimaCobrancaEm, UltimaMensagem,
             ValorCobrado, EmAbertoNoFechamento, SituacaoFinal, EstadoFila, NotaFiscal, NFEmitida, NaoCobrarNoCiclo,
             Incobravel, FechadoEm)
        WITH chaves AS (
            SELECT CodigoCliente FROM {V_PENDENCIAS} WHERE MesCiclo = @mes
            UNION DISTINCT SELECT CodigoCliente FROM {T_ENVIOS} WHERE MesReferencia = @mes
            UNION DISTINCT SELECT CodigoCliente FROM {T_CONTATOS} WHERE NFEmitidaNoCiclo = @mes OR NaoCobrarNoCiclo = @mes
        ),
        env AS (
            SELECT CodigoCliente, COUNT(*) AS QtdEnvios, MIN(EnviadoEm) AS Primeira, MAX(EnviadoEm) AS Ultima,
                   ARRAY_AGG(MensagemNome ORDER BY EnviadoEm DESC LIMIT 1)[OFFSET(0)] AS UltimaMensagem,
                   ARRAY_AGG(ValorNoEnvio ORDER BY EnviadoEm LIMIT 1)[OFFSET(0)] AS ValorCobrado
            FROM {T_ENVIOS} WHERE MesReferencia = @mes GROUP BY CodigoCliente
        ),
        cad AS (SELECT CAST(Codigo AS INT64) AS Codigo, ANY_VALUE(Nome) AS Nome FROM {T_CLIENTS} GROUP BY 1)
        SELECT @mes, k.CodigoCliente, COALESCE(p.NomeCliente, cad.Nome),
               COALESCE(e.QtdEnvios, 0), e.Primeira, e.Ultima, e.UltimaMensagem, e.ValorCobrado,
               COALESCE(p.TotalEmAberto, 0), IF(p.CodigoCliente IS NULL, 'QUITADO', 'EM_ABERTO'), p.EstadoFila,
               c.NotaFiscal, COALESCE(c.NFEmitidaNoCiclo = @mes, FALSE), COALESCE(c.NaoCobrarNoCiclo = @mes, FALSE),
               COALESCE(c.Situacao = 'INCOBRAVEL', FALSE), CURRENT_TIMESTAMP()
        FROM chaves k
        LEFT JOIN env e ON e.CodigoCliente = k.CodigoCliente
        LEFT JOIN {V_PENDENCIAS} p ON p.CodigoCliente = k.CodigoCliente AND p.MesCiclo = @mes
        LEFT JOIN {T_CONTATOS} c ON c.CodigoCliente = k.CodigoCliente
        LEFT JOIN cad ON cad.Codigo = k.CodigoCliente
        WHERE NOT EXISTS (SELECT 1 FROM {T_HISTORICO} h WHERE h.MesReferencia = @mes AND h.CodigoCliente = k.CodigoCliente)""",
                mes=("DATE", mes))
    _q(client, f"UPDATE {T_CICLOS} SET FechadoEm = IFNULL(FechadoEm, CURRENT_TIMESTAMP()) WHERE MesReferencia = @mes",
       mes=("DATE", mes))
    return n


def iniciar_novo_ciclo(client, mes: date) -> dict:
    """O que o botao 'Iniciar cobranca' chama: PRIMEIRO fotografa o ciclo atual (se houver), DEPOIS inicia o novo.
    Se a foto falhar, o novo ciclo NAO e iniciado (a excecao sobe) — assim nunca se perde o historico."""
    atual = ciclo_atual(client)
    a_fechar = atual if atual and atual != mes else None
    fotografados = fechar_ciclo(client, a_fechar) if a_fechar else 0
    return {"fotografados": fotografados, "ciclo_fechado": a_fechar, "criado": iniciar_ciclo(client, mes)}


def ciclos_com_foto(client) -> list[date]:
    return [r["MesReferencia"] for r in _q(
        client, f"SELECT DISTINCT MesReferencia FROM {T_HISTORICO} ORDER BY MesReferencia DESC")]


def historico_df(client, mes: date) -> pd.DataFrame:
    return _q(client, f"""
        SELECT * FROM {T_HISTORICO} WHERE MesReferencia = @mes
        ORDER BY (SituacaoFinal = 'EM_ABERTO') DESC, EmAbertoNoFechamento DESC, NomeCliente""",
              mes=("DATE", mes)).to_dataframe()


def envios_do_ciclo(client, mes: date) -> pd.DataFrame:
    """Registro completo dos envios do ciclo (funciona para o ciclo atual e para os antigos)."""
    return _q(client, f"""
        SELECT e.EnviadoEm, e.CodigoCliente, cl.Nome AS NomeCliente, e.MensagemNome, e.ValorNoEnvio, e.TextoEnviado
        FROM {T_ENVIOS} e
        LEFT JOIN (SELECT CAST(Codigo AS INT64) AS Codigo, ANY_VALUE(Nome) AS Nome FROM {T_CLIENTS} GROUP BY 1) cl
               ON cl.Codigo = e.CodigoCliente
        WHERE e.MesReferencia = @mes ORDER BY e.EnviadoEm""", mes=("DATE", mes)).to_dataframe()


def fila(client) -> pd.DataFrame:
    return _q(client, f"""
        SELECT * FROM {V_PENDENCIAS}
        ORDER BY CASE EstadoFila WHEN 'A_COBRAR' THEN 1 WHEN 'COBRADO' THEN 2
                                 WHEN 'NAO_COBRAR_NO_CICLO' THEN 3 ELSE 4 END,
                 TotalEmAberto DESC, NomeCliente""").to_dataframe()


def sessoes_cliente(client, codigo: int) -> pd.DataFrame:
    return _q(client, f"""
        SELECT DataSessao, NomeAnimal, ProdutoServico, Valor, Parcial
        FROM {V_SESSOES} WHERE CodigoCliente = @codigo ORDER BY DataSessao, Venda""",
              codigo=("INT64", int(codigo))).to_dataframe()


def sessoes_clientes(client, codigos: list[int]) -> pd.DataFrame:
    """Sessoes em aberto de VARIOS clientes numa query so (para montar as mensagens)."""
    cfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("codigos", "INT64", [int(c) for c in codigos])])
    return client.query(f"""
        SELECT CodigoCliente, DataSessao, NomeAnimal, ProdutoServico, Valor, Parcial
        FROM {V_SESSOES} WHERE CodigoCliente IN UNNEST(@codigos)
        ORDER BY CodigoCliente, DataSessao, Venda""", job_config=cfg).result().to_dataframe()


def mensagens_df(client, tabela: str | None = None) -> pd.DataFrame:
    t = tabela or T_MENSAGENS
    return _q(client, f"SELECT Nome, Texto, EhInicial, Ativo, AtualizadoEm FROM {t} ORDER BY EhInicial DESC, Nome").to_dataframe()


def salvar_mensagem(client, nome: str, texto: str, ativo: bool = True, eh_inicial: bool = False,
                    tabela: str | None = None) -> None:
    """Cria ou atualiza um modelo (MERGE pela chave Nome). So uma mensagem pode ser a inicial: marcar esta
    desmarca as outras. Modelos nao sao apagados (envios guardam o nome); desative com ativo=False.
    `tabela` so existe para os testes usarem uma copia descartavel."""
    t = tabela or T_MENSAGENS
    nome = (nome or "").strip()
    if not nome or len(nome) > 60:
        raise ValueError("Dê um nome à mensagem (até 60 caracteres).")
    if not (texto or "").strip():
        raise ValueError("O texto da mensagem não pode ficar vazio.")
    params = dict(nome=("STRING", nome), texto=("STRING", texto), ini=("BOOL", bool(eh_inicial)), ativo=("BOOL", bool(ativo)))
    _q(client, f"""
        MERGE {t} T USING (SELECT @nome AS Nome) S ON T.Nome = S.Nome
        WHEN MATCHED THEN UPDATE SET Texto = @texto, EhInicial = @ini, Ativo = @ativo, AtualizadoEm = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (Nome, Texto, EhInicial, Ativo, AtualizadoEm)
             VALUES (@nome, @texto, @ini, @ativo, CURRENT_TIMESTAMP())""", **params)
    if eh_inicial:
        _q(client, f"UPDATE {t} SET EhInicial = FALSE, AtualizadoEm = CURRENT_TIMESTAMP() WHERE Nome <> @nome AND EhInicial",
           nome=("STRING", nome))


def registrar_envio(client, codigo: int, ciclo: date, mensagem_nome: str, nome_contato: str | None,
                    telefone: str | None, valor, texto: str) -> str:
    """Grava que a mensagem foi enviada (o usuario confirma; abrir o link nao grava nada). Devolve o EnvioId."""
    envio_id = uuid.uuid4().hex
    _q(client, f"""
        INSERT INTO {T_ENVIOS}
            (EnvioId, CodigoCliente, MesReferencia, MensagemNome, EnviadoEm, NomeContato, TelefoneWhatsapp, ValorNoEnvio, TextoEnviado)
        VALUES (@id, @codigo, @ciclo, @msg, CURRENT_TIMESTAMP(), @nome, @tel, @valor, @texto)""",
       id=("STRING", envio_id), codigo=("INT64", int(codigo)), ciclo=("DATE", ciclo), msg=("STRING", mensagem_nome),
       nome=("STRING", nome_contato), tel=("STRING", telefone),
       valor=("NUMERIC", Decimal(str(round(float(valor), 2)))), texto=("STRING", texto))
    return envio_id


def desfazer_ultimo_envio(client, codigo: int, ciclo: date) -> int:
    """Apaga o envio mais recente do cliente neste ciclo (para corrigir um 'marcar como enviado' por engano).
    E a unica excecao a 'envios so recebe insercao'. Devolve quantas linhas apagou."""
    return _dml(client, f"""
        DELETE FROM {T_ENVIOS}
        WHERE EnvioId = (SELECT EnvioId FROM {T_ENVIOS}
                         WHERE CodigoCliente = @codigo AND MesReferencia = @ciclo
                         ORDER BY EnviadoEm DESC LIMIT 1)""",
                codigo=("INT64", int(codigo)), ciclo=("DATE", ciclo))


def contatos_df(client) -> pd.DataFrame:
    return _q(client, f"""
        SELECT c.CodigoCliente, cl.Nome AS NomeCliente, an.Animais, c.NomeContato, c.TelefoneWhatsapp,
               c.Situacao, c.NotaFiscal, c.Observacao, c.RevisadoEm IS NOT NULL AS Revisado
        FROM {T_CONTATOS} c
        LEFT JOIN (SELECT CAST(Codigo AS INT64) AS Codigo, ANY_VALUE(Nome) AS Nome
                   FROM {T_CLIENTS} GROUP BY 1) cl ON cl.Codigo = c.CodigoCliente
        LEFT JOIN (SELECT CAST(Cliente_Codigo AS INT64) AS Codigo,
                          -- todos os animais do cliente numa linha: "Mel / Thor"; falecidos vao por ultimo, com cruz
                          STRING_AGG(CONCAT(Nome, IF(VivoMorto LIKE '%bito%', ' †', '')), ' / '
                                     ORDER BY IF(VivoMorto LIKE '%bito%', 1, 0), Nome) AS Animais
                   FROM {T_ANIMALS} GROUP BY 1) an ON an.Codigo = c.CodigoCliente
        ORDER BY Revisado, NomeCliente""").to_dataframe()


def salvar_contato(client, codigo: int, nome: str, telefone, observacao: str | None, revisado: bool) -> None:
    """Cria ou atualiza o contato (MERGE). Telefone e normalizado; ValueError se invalido."""
    tel = normalizar_telefone(telefone)
    nome = (nome or "").strip() or None
    obs = (observacao or "").strip() or None
    _q(client, f"""
        MERGE {T_CONTATOS} T
        USING (SELECT @codigo AS CodigoCliente) S ON T.CodigoCliente = S.CodigoCliente
        WHEN MATCHED THEN UPDATE SET
            NomeContato = @nome, TelefoneWhatsapp = @tel, Observacao = @obs,
            RevisadoEm = IF(@revisado, IFNULL(T.RevisadoEm, CURRENT_TIMESTAMP()), NULL),
            AtualizadoEm = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (CodigoCliente, NomeContato, TelefoneWhatsapp, Situacao, NaoCobrarNoCiclo, Observacao, RevisadoEm, AtualizadoEm)
            VALUES (@codigo, @nome, @tel, 'ATIVO', NULL, @obs,
                    IF(@revisado, CURRENT_TIMESTAMP(), NULL), CURRENT_TIMESTAMP())""",
       codigo=("INT64", int(codigo)), nome=("STRING", nome), tel=("STRING", tel),
       obs=("STRING", obs), revisado=("BOOL", bool(revisado)))


def _garantir_contato(client, codigo: int) -> None:
    """Se o cliente ainda nao tem linha em contatos, cria uma a partir do cadastro do Simples Vet."""
    _q(client, f"""
        INSERT INTO {T_CONTATOS}
            (CodigoCliente, NomeContato, TelefoneWhatsapp, Situacao, NaoCobrarNoCiclo, Observacao, RevisadoEm, AtualizadoEm)
        SELECT CAST(c.Codigo AS INT64), INITCAP(SPLIT(TRIM(c.Nome), ' ')[SAFE_OFFSET(0)]), {_SQL_TEL_CADASTRO}, 'ATIVO',
               CAST(NULL AS DATE), CAST(NULL AS STRING), CAST(NULL AS TIMESTAMP), CURRENT_TIMESTAMP()
        FROM {T_CLIENTS} c
        WHERE CAST(c.Codigo AS INT64) = @codigo
          AND NOT EXISTS (SELECT 1 FROM {T_CONTATOS} x WHERE x.CodigoCliente = @codigo)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY c.Codigo ORDER BY c.Nome) = 1""",
       codigo=("INT64", int(codigo)))


def definir_situacao(client, codigo: int, situacao: str) -> None:
    """ATIVO ou INCOBRAVEL (permanente: some da fila em todos os ciclos ate ser reativado)."""
    if situacao not in SITUACOES:
        raise ValueError(f"Situação inválida: {situacao}")
    _garantir_contato(client, codigo)
    _q(client, f"UPDATE {T_CONTATOS} SET Situacao = @s, AtualizadoEm = CURRENT_TIMESTAMP() WHERE CodigoCliente = @codigo",
       s=("STRING", situacao), codigo=("INT64", int(codigo)))


def definir_nota_fiscal(client, codigo: int, valor: str | None) -> None:
    """COM_CPF, SEM_CPF ou None (nao precisa de NF). Tirar a NF tambem apaga a marca de 'emitida'."""
    if valor not in (None, "COM_CPF", "SEM_CPF"):
        raise ValueError(f"Nota fiscal inválida: {valor}")
    _garantir_contato(client, codigo)
    _q(client, f"""UPDATE {T_CONTATOS}
                   SET NotaFiscal = @v, NFEmitidaNoCiclo = IF(@v IS NULL, NULL, NFEmitidaNoCiclo),
                       AtualizadoEm = CURRENT_TIMESTAMP()
                   WHERE CodigoCliente = @codigo""",
       v=("STRING", valor), codigo=("INT64", int(codigo)))


def definir_nf_emitida(client, codigo: int, ciclo: date | None) -> int:
    """Marca a NF como emitida no ciclo (expira sozinha no ciclo seguinte); None desmarca.
    So vale para quem tem NF configurada. Devolve quantas linhas mudou (0 = cliente sem NF, ignorado)."""
    so_com_nf = "" if ciclo is None else " AND NotaFiscal IS NOT NULL"
    return _dml(client, f"""UPDATE {T_CONTATOS}
                            SET NFEmitidaNoCiclo = @ciclo, AtualizadoEm = CURRENT_TIMESTAMP()
                            WHERE CodigoCliente = @codigo{so_com_nf}""",
                ciclo=("DATE", ciclo), codigo=("INT64", int(codigo)))


def definir_nao_cobrar(client, codigo: int, ciclo: date | None) -> None:
    """Marca 'devendo, nao cobrar' so para o ciclo informado (expira sozinho no ciclo seguinte). None desfaz."""
    _garantir_contato(client, codigo)
    _q(client, f"UPDATE {T_CONTATOS} SET NaoCobrarNoCiclo = @ciclo, AtualizadoEm = CURRENT_TIMESTAMP() WHERE CodigoCliente = @codigo",
       ciclo=("DATE", ciclo), codigo=("INT64", int(codigo)))
