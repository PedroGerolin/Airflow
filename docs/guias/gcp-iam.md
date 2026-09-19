# Guia de estudo — GCP: permissões (IAM), service accounts e BigQuery

Os **comandos exatos** ficam em [`gcp_setup/README.md`](../../gcp_setup/README.md). Aqui, os conceitos.

## Conceitos

- **Projeto** (`gerolingcp`): a "caixa" que contém tudo (BigQuery, buckets, service accounts).
- **IAM** responde a: *quem* (membro) pode fazer *o quê* (role) em *qual recurso*.
  - **Membro**: usuário (`user:…`) ou **service account** (`serviceAccount:…`) — uma "conta de
    robô" usada por programas (Airflow, dbt, Metabase).
  - **Role**: pacote de permissões. *Básicas* (`owner`, `editor`, `viewer`) são amplas demais;
    *predefinidas* (`bigquery.dataViewer`, `bigquery.jobUser`, `storage.objectAdmin`…) são
    específicas. Existem também custom roles.
- **Menor privilégio**: dar só o que o programa realmente usa. A SA do ETL tinha `roles/editor`
  (praticamente tudo no projeto); trocamos por `bigquery.dataEditor` + `bigquery.jobUser`
  (projeto) e `storage.objectAdmin` **só no bucket** do pipeline. Se a chave vazar, o estrago é
  limitado.
- **Escopo do grant**: a mesma role vale coisas diferentes conforme onde é concedida — no
  **projeto** (vale pra todos os datasets/buckets) ou num **recurso** (só naquele dataset/bucket).
  A SA do Metabase lê apenas `FisioVet` e `FisioVet_Analytics`, não os outros datasets do projeto.
- **`jobUser` ≠ `dataViewer`**: `dataViewer` permite *ler* tabelas; `jobUser` permite *executar
  queries* (jobs). Pra consultar você precisa dos dois.
- **Chave JSON de service account**: credencial de longa duração (um arquivo). Fica em
  `credential/` (gitignorado); nunca no chat, no git ou em print. Quanto menos chaves, melhor —
  quando possível prefira identidade sem chave; aqui, como o Airflow e o Metabase rodam
  localmente, a chave é o caminho prático.

## Como isso apareceu no projeto
- **Ordem ao apertar permissões**: conceder o novo **antes** de remover o antigo. Depois de
  trocar `editor` por permissões específicas, rodamos a DAG inteira pra provar que nada quebrou.
- **O MCP `toolbox-bigquery` autentica como a SA do ETL** — por isso ele *não consegue* alterar
  IAM de datasets (`bigquery.datasets.update` negado). Mudanças de permissão precisam da conta
  owner via `gcloud`/`bq`. É o sistema funcionando como deveria.
- **BigQuery DCL**: dá pra conceder permissão de dataset com SQL:
  ``GRANT `roles/bigquery.dataViewer` ON SCHEMA `gerolingcp.FisioVet` TO 'serviceAccount:…'``
  (no PowerShell, use **aspas simples** no literal; as duplas somem ao chamar o `bq.cmd`).
- **O Snowflake também vira "membro" do GCP**: a *storage integration* gera uma service account
  do Google; você concede `objectViewer` no bucket a ela. Muda a cada conta Snowflake.
- **Tabela externa do BigQuery lê o GCS com as credenciais de quem consulta**, então a SA que
  consulta também precisa de leitura no bucket.

## Comandos de diagnóstico
```powershell
gcloud auth list                                   # quem está logado
gcloud iam service-accounts list --project gerolingcp
gcloud projects get-iam-policy gerolingcp --flatten="bindings[].members" `
  --format="table(bindings.role)" --filter="bindings.members:<email da SA>"   # roles da SA no projeto
gcloud storage buckets get-iam-policy gs://gerolin_etl                        # quem acessa o bucket
bq show --format=prettyjson gerolingcp:FisioVet                               # acesso a nível de dataset
```
