# Setup de IAM no GCP (projeto `gerolingcp`)

Comandos executados (setembro/2026) pra deixar as permissões documentadas e reproduzíveis.
Nenhum segredo aqui: chaves JSON ficam em `credential/` (gitignorado).

No Windows, o `gcloud`/`bq` instalados via winget podem não estar no PATH da sessão; usar o
caminho completo: `%LOCALAPPDATA%\Google\Cloud SDK\google-cloud-sdk\bin\gcloud.cmd` (e `bq.cmd`).
Autenticar uma vez com `gcloud auth login` (conta owner) e `gcloud config set project gerolingcp`.

## 1. Service Account do ETL (`gerolin-service-account`) — menor privilégio

Usada pelo Airflow/dbt (chave `credential/gerolingcp-*.json`). Tinha `roles/editor` (projeto
inteiro); trocado por permissões escopadas. Ordem: **conceder o novo antes de remover o antigo**.

```powershell
$sa = "serviceAccount:gerolin-service-account@gerolingcp.iam.gserviceaccount.com"
# leitura/escrita apenas no bucket do pipeline
gcloud storage buckets add-iam-policy-binding gs://gerolin_etl --member=$sa --role="roles/storage.objectAdmin"
# criar/atualizar tabelas no BigQuery (dbt) + poder rodar jobs/queries
gcloud projects add-iam-policy-binding gerolingcp --member=$sa --role="roles/bigquery.dataEditor" --condition=None
gcloud projects add-iam-policy-binding gerolingcp --member=$sa --role="roles/bigquery.jobUser" --condition=None
# remocao do que era amplo/redundante
gcloud projects remove-iam-policy-binding gerolingcp --member=$sa --role="roles/editor" --condition=None
gcloud projects remove-iam-policy-binding gerolingcp --member=$sa --role="roles/bigquery.dataViewer" --condition=None
gcloud projects remove-iam-policy-binding gerolingcp --member=$sa --role="roles/mcp.toolUser" --condition=None
```
Estado final: `bigquery.dataEditor` + `bigquery.jobUser` (projeto) e `storage.objectAdmin` (só no bucket).
Como o MCP `toolbox-bigquery` autentica como essa SA, ele **não consegue** alterar IAM de datasets
(`bigquery.datasets.update` negado) — mudanças de permissão têm que ser feitas com a conta owner via `gcloud`/`bq`.

## 2. Identidade que o Snowflake gera pra ler o bucket (storage integration)

O `CREATE STORAGE INTEGRATION` (ver `dags/FisioVet/.dbt/snowflake_setup/02_*.sql`) gera uma
service account do Google **diferente a cada conta/trial**. Depois do `DESC STORAGE INTEGRATION
gerolin_fisiovet;` pegar `STORAGE_GCP_SERVICE_ACCOUNT` e conceder leitura:
```powershell
gcloud storage buckets add-iam-policy-binding gs://gerolin_etl --member="serviceAccount:<SA_GERADA_PELO_SNOWFLAKE>" --role="roles/storage.objectViewer"
```
Conta atual (trial de 09/2026): `kzao50000@prod3-f617.iam.gserviceaccount.com`.
**Pendência de limpeza:** o bucket ainda tem o grant da integration da trial antiga
(`squtmsjikk@prod3-f617...`, custom role `projects/gerolingcp/roles/SnowflakeRole`) — órfão,
pode ser removido.

## 3. Service Account só-leitura do Metabase (`metabase-reader`)

Acesso só aos datasets do dbt (`FisioVet`, `FisioVet_Analytics`) — não enxerga os datasets
`Cruzeiro_do_Sul_*` que existem no mesmo projeto.
```powershell
gcloud iam service-accounts create metabase-reader --project gerolingcp --display-name="Metabase (somente leitura)"
gcloud projects add-iam-policy-binding gerolingcp --member="serviceAccount:metabase-reader@gerolingcp.iam.gserviceaccount.com" --role="roles/bigquery.jobUser" --condition=None

# permissao de leitura por dataset (DCL). Rodar com a conta OWNER via bq, e usar ASPAS SIMPLES
# no literal — o PowerShell engole as aspas duplas ao chamar o bq.cmd.
$bq = "$env:LOCALAPPDATA\Google\Cloud SDK\google-cloud-sdk\bin\bq.cmd"
foreach ($ds in @("FisioVet","FisioVet_Analytics")) {
    $sql = "GRANT ``roles/bigquery.dataViewer`` ON SCHEMA ``gerolingcp.$ds`` TO 'serviceAccount:metabase-reader@gerolingcp.iam.gserviceaccount.com'"
    & $bq query --project_id=gerolingcp --location=US --nouse_legacy_sql $sql
}

# chave JSON (fica em credential/, gitignorado; o conteudo nunca vai pro chat/git)
gcloud iam service-accounts keys create "C:\Airflow\credential\metabase-reader.json" --iam-account="metabase-reader@gerolingcp.iam.gserviceaccount.com" --project gerolingcp
```
Verificar: `bq show --format=prettyjson gerolingcp:FisioVet` deve listar `role: READER` pra essa SA.

## 4. App de cobranca (Streamlit): dataset `FisioVet_App` + Service Account `cobranca-app`

Desenho e regras: `ROADMAP.md` (secao "Cobranca por WhatsApp"), na pasta de estudos do usuario (fora do repo).
Dataset **separado** de `FisioVet_Analytics` de proposito: o dbt recria as tabelas do Analytics, entao
nada que o usuario edita pode morar la. Os dados (contatos, envios) **nunca** vao pro repo; so o SQL.

Comandos executados em 21/09/2026 (Git Bash, conta owner). `bq`/`gcloud` no PATH.
```bash
# dataset (mesma regiao dos outros: US)
bq mk --dataset --project_id=gerolingcp --location=US --description="App de cobranca (Streamlit): tabelas escritas pelo app. O dbt nao escreve aqui." gerolingcp:FisioVet_App

# service account do app + permissoes minimas
gcloud iam service-accounts create cobranca-app --project gerolingcp --display-name="App de cobranca (Streamlit)"
SA="cobranca-app@gerolingcp.iam.gserviceaccount.com"
gcloud projects add-iam-policy-binding gerolingcp --member="serviceAccount:$SA" --role="roles/bigquery.jobUser" --condition=None
# escrita SO no dataset do app; leitura nos dois datasets do pipeline (DCL, com a conta owner)
bq query --project_id=gerolingcp --location=US --nouse_legacy_sql "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`gerolingcp.FisioVet_App\` TO 'serviceAccount:$SA'"
for ds in FisioVet FisioVet_Analytics; do
  bq query --project_id=gerolingcp --location=US --nouse_legacy_sql "GRANT \`roles/bigquery.dataViewer\` ON SCHEMA \`gerolingcp.$ds\` TO 'serviceAccount:$SA'"
done

# tabelas e dados iniciais (scripts versionados em gcp_setup/sql/, rodar nesta ordem)
for f in 01_fisiovet_app_tables 02_seed_mensagens 03_seed_contatos 04_ajuste_como_chamar_e_animais 05_nota_fiscal 06_sem_pix 07_historico_de_ciclos; do
  SQL=$'\n'"$(cat gcp_setup/sql/$f.sql)"      # ver armadilha 1 abaixo
  bq query --project_id=gerolingcp --location=US --nouse_legacy_sql "$SQL"
done

# chave JSON (fica em credential/, gitignorado; o conteudo nunca vai pro chat/git)
gcloud iam service-accounts keys create "C:\Airflow\credential\cobranca-app.json" --iam-account="$SA" --project gerolingcp
```
Estado final: `cobranca-app` = `bigquery.jobUser` (projeto) + `dataEditor` em `FisioVet_App` + `dataViewer` em
`FisioVet` e `FisioVet_Analytics`. Verificado usando a propria chave: le os 3 datasets, escreve so em
`FisioVet_App`, e leva `403 Permission bigquery.tables.updateData denied` ao tentar escrever nos outros dois.

**Armadilhas encontradas**
1. `bq query` trata um argumento que **comeca com `--`** como flag: o SQL comeca com comentario `--`, entao
   prefixe uma quebra de linha (`$'\n'"$(cat arquivo)"`). Passar por argumento (e nao por stdin) manteve os acentos.
2. No BigQuery, `NOT NULL DEFAULT 'x'` na mesma coluna e erro de sintaxe; o app preenche os valores, sem `DEFAULT`.
3. O `04_*.sql` (21/09) troca o nome do contato pelo primeiro nome e o marcador `{nome_cliente}` por `{animais}` nos rascunhos; os seeds 02/03 ja nascem assim numa recriacao do zero.
4. O `05_nota_fiscal.sql` (21/09) adiciona `NotaFiscal` e `NFEmitidaNoCiclo` a `contatos` (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`: so metadado, linhas existentes ficam NULL); o `01` ja cria as colunas numa recriacao do zero.
5. O `03_seed_contatos.sql` roda `INSERT ... SELECT` dentro do BigQuery: telefones de clientes nao passam por
   arquivo nem pelo repo. E idempotente (`NOT EXISTS`).
Recriar em outro projeto: trocar `gerolingcp` nos comandos e nos `.sql`, e rodar tudo de novo.
