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
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "gerolingcp")
T_CONTATOS = f"`{PROJECT}.FisioVet_App.contatos`"
T_CICLOS = f"`{PROJECT}.FisioVet_App.ciclos`"
V_PENDENCIAS = f"`{PROJECT}.FisioVet_Analytics.cobranca_pendencias`"
V_SESSOES = f"`{PROJECT}.FisioVet_Analytics.cobranca_sessoes`"
T_CLIENTS = f"`{PROJECT}.FisioVet.clients`"
T_ANIMALS = f"`{PROJECT}.FisioVet.animals`"

TZ = ZoneInfo("America/Sao_Paulo")
SITUACOES = ("ATIVO", "INCOBRAVEL")
ESTADOS = ("A_COBRAR", "COBRADO", "NAO_COBRAR_NO_CICLO", "INCOBRAVEL")
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


def resumo_por_estado(fila: pd.DataFrame) -> dict:
    """{estado: (quantidade de clientes, total em aberto)} para os 4 estados (zeros incluidos)."""
    saida = {}
    for estado in ESTADOS:
        parte = fila[fila["EstadoFila"] == estado] if not fila.empty else fila
        total = float(parte["TotalEmAberto"].sum()) if not parte.empty else 0.0
        saida[estado] = (len(parte), total)
    return saida


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


def contatos_df(client) -> pd.DataFrame:
    return _q(client, f"""
        SELECT c.CodigoCliente, cl.Nome AS NomeCliente, an.Animais, c.NomeContato, c.TelefoneWhatsapp,
               c.Situacao, c.Observacao, c.RevisadoEm IS NOT NULL AS Revisado
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


def definir_nao_cobrar(client, codigo: int, ciclo: date | None) -> None:
    """Marca 'devendo, nao cobrar' so para o ciclo informado (expira sozinho no ciclo seguinte). None desfaz."""
    _garantir_contato(client, codigo)
    _q(client, f"UPDATE {T_CONTATOS} SET NaoCobrarNoCiclo = @ciclo, AtualizadoEm = CURRENT_TIMESTAMP() WHERE CodigoCliente = @codigo",
       ciclo=("DATE", ciclo), codigo=("INT64", int(codigo)))
