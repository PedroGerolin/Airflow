"""Backup das tabelas ESCRITAS PELO APP de cobranca (dataset FisioVet_App) para o GCS, em Parquet.

Por que: vendas, clientes e faturamento sao refeitos todo dia pelo pipeline, mas contatos, envios, ciclos,
ciclos_historico e mensagens sao o TRABALHO do usuario e nao existem em nenhum outro lugar. A unica protecao
que o BigQuery da sozinho e o historico de 7 dias (time travel).

Onde: gs://gerolin_etl/_backup/FisioVet_App/AAAA-MM-DD/<tabela>/part-*.parquet  (uma pasta por dia; rodar duas
vezes no mesmo dia sobrescreve). O prefixo `_backup/` nao e lido por nenhuma tabela externa. Uma regra de ciclo de
vida do bucket apaga essas pastas depois de 90 dias (ver gcp_setup/README.md, secao 5).
Restaurar: gcp_setup/README.md, secao 5 (bq load --source_format=PARQUET).
"""
import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

PROJECT = "gerolingcp"
DATASET = "FisioVet_App"
BUCKET = "gerolin_etl"
PREFIXO = "_backup/FisioVet_App"
TABELAS = ["contatos", "ciclos", "ciclos_historico", "envios", "mensagens", "configuracoes"]


def backup_app_tables(gcp_conn_id: str = "google_cloud_default", hoje: datetime.date | None = None) -> dict:
    """Exporta cada tabela do app para o GCS e CONFERE que os arquivos apareceram. Devolve {tabela: linhas}.
    Levanta erro se uma tabela com linhas nao gerou arquivo (o backup nao pode falhar em silencio)."""
    hoje = hoje or datetime.date.today()
    bq = BigQueryHook(gcp_conn_id=gcp_conn_id).get_client(project_id=PROJECT)
    gcs = GCSHook(gcp_conn_id=gcp_conn_id)
    resumo = {}
    for tabela in TABELAS:
        nome = f"`{PROJECT}.{DATASET}.{tabela}`"
        linhas = list(bq.query(f"SELECT COUNT(*) AS n FROM {nome}", location="US").result())[0]["n"]
        resumo[tabela] = linhas
        if linhas == 0:
            print(f"[backup] {tabela}: vazia, nada a copiar")
            continue
        pasta = f"{PREFIXO}/{hoje:%Y-%m-%d}/{tabela}/"
        bq.query(
            f"EXPORT DATA OPTIONS(uri='gs://{BUCKET}/{pasta}part-*.parquet', format='PARQUET', overwrite=true) "
            f"AS SELECT * FROM {nome}", location="US").result()
        arquivos = gcs.list(BUCKET, prefix=pasta)
        if not arquivos:
            raise RuntimeError(f"Backup de {tabela}: {linhas} linha(s) mas nenhum arquivo em gs://{BUCKET}/{pasta}")
        print(f"[backup] {tabela}: {linhas} linha(s) -> {len(arquivos)} arquivo(s) em gs://{BUCKET}/{pasta}")
    return resumo
