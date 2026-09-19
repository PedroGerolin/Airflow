# Runbook — recriar tudo numa conta Snowflake nova (fim do trial)

Objetivo: se a conta trial acabar (ou for trocada), refazer a infraestrutura do zero, na ordem
certa, **sem depender de memória**. Tempo estimado: 45–90 min, a maior parte esperando dbt/queries.
Cada passo aponta pro script/guia versionado. Conceitos por trás: [`guias/snowflake.md`](guias/snowflake.md).

> Convenção: o que roda no Snowflake roda como **ACCOUNTADMIN** (é dono dos schemas; rodar como
> SYSADMIN falha). Comandos com `<…>` precisam de valor novo.

## Passo 0 — Anotar os dados da conta nova
- **Identificador da conta** `ORGANIZACAO-CONTA` (Snowsight → menu da conta → copiar identificador).
- **Usuário** que vai usar (ex.: o seu, criado no cadastro da trial).
- Conferir que existe um warehouse (`SHOW WAREHOUSES;`, esperado `COMPUTE_WH`; se o nome mudar,
  ajustar `profiles.yml`, `06_*.sql` e o MCP).

## Passo 1 — Autenticação por chave (sem senha)
Reaproveite o **mesmo par de chaves** (a privada em `credential/snowflake_rsa_key.p8` continua
valendo); só registre a pública no usuário da conta nova:
```powershell
(Get-Content C:\Airflow\credential\snowflake_rsa_key.pub | Where-Object { $_ -notmatch "^-----" }) -join ""
```
```sql
ALTER USER <usuario> SET RSA_PUBLIC_KEY='<saída do comando acima>';
```
Se a chave se perdeu, gere outra (comandos em `dags/FisioVet/.dbt/snowflake_setup/README.md`,
seção "Autenticação por chave"). A passphrase é a variável de usuário
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

## Passo 2 — Configurar o dbt
1. Copie `dags/FisioVet/.dbt/profiles.yml.example` para `profiles.yml` (gitignorado).
2. Troque `account:` e `user:` do target `dev_snowflake`. O resto não muda.
3. Confira que `credential/snowflake_rsa_key.p8` existe e que a passphrase está na variável de usuário.

## Passo 3 — Infraestrutura (scripts numerados, nesta ordem)
Pasta `dags/FisioVet/.dbt/snowflake_setup/`. Rode no Snowsight (ou via MCP, com cada comando em
**uma linha só** — o classificador do Claude Code bloqueou SQL multi-linha uma vez).

| # | Script | O que faz | Observação |
|---|---|---|---|
| 1 | `01_schemas.sql` | database + 3 schemas | |
| 2 | `02_storage_integration.sql` | integration com o GCS | **PARADA OBRIGATÓRIA:** rode `DESC STORAGE INTEGRATION gerolin_fisiovet;`, copie `STORAGE_GCP_SERVICE_ACCOUNT` e conceda leitura no bucket (comando em `gcp_setup/README.md`, seção 2). Sem isso o passo 3 falha. |
| 3 | `03_stage_and_file_formats.sql` | stage + file formats | Termina com `LIST @…` — deve listar os CSVs. |
| 4 | `04_external_tables.sql` | 3 tabelas externas | Se o layout do CSV do simples.vet mudou, ajustar as posições `cN` antes (compare com a tabela externa do BigQuery). |
| 5 | `05_reference_data.sql` | `services`, `commission`, `debts_types` | Dados de negócio mantidos manualmente. |

## Passo 4 — Materializar com o dbt e validar
```powershell
$env:SNOWFLAKE_PRIVATE_KEY_PATH = "C:\Airflow\credential\snowflake_rsa_key.p8"
$env:SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = [Environment]::GetEnvironmentVariable('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE','User')
Remove-Item -Recurse -Force dags\FisioVet\.dbt\target -ErrorAction SilentlyContinue
.venv\Scripts\dbt.exe run --target dev_snowflake --profiles-dir dags/FisioVet/.dbt --project-dir dags/FisioVet/.dbt
```
Esperado: `PASS=10` (3 hooks de `REFRESH` + 7 modelos). **Paridade com o BigQuery** (rodar nos dois e comparar):
```sql
SELECT MAX(date), COUNT(DISTINCT date) FROM FISIOVET.FISIOVET.SALES;      -- Snowflake
-- BigQuery: SELECT MAX(date), COUNT(DISTINCT date) FROM `gerolingcp.FisioVet.sales`;
SELECT (SELECT COUNT(*) FROM SERVICES), (SELECT COUNT(*) FROM COMMISSION), (SELECT COUNT(*) FROM DEBTS_TYPES);  -- 14, 84, 13
```
Também rodar a DAG uma vez (`scripts/run_daily_pipeline.ps1`) pra confirmar o `dbt_run_snowflake`
dentro do container (o caminho da chave lá é `/opt/airflow/credential/…`).

## Passo 5 — Metabase (só-leitura)
1. Rodar `06_metabase_reader.sql` (role, grants, usuário).
2. Gerar a chave **sem criptografia** do Metabase e registrar a pública (`ALTER USER METABASE SET TYPE = SERVICE RSA_PUBLIC_KEY='…'`).
3. No Metabase: Admin → Bancos de dados → editar a conexão Snowflake (novo `account`) e reenviar a chave.

## Passo 6 — MCP `toolbox-snowflake` (opcional)
Editar `~/.claude.json` → `projects["c:/Airflow"].mcpServers.toolbox-snowflake.env`
(`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`). Ele só suporta **senha** (`SNOWFLAKE_PASSWORD`,
variável de usuário do Windows) — ver a pendência no `CLAUDE.md`.

## Passo 7 — Limpeza da conta antiga
- No bucket `gerolin_etl`, remover o grant da service account da integration antiga
  (`gcloud storage buckets remove-iam-policy-binding gs://gerolin_etl --member="serviceAccount:<SA antiga>" --role=<role>`).
- Se a conta antiga ainda existir, revogar/derrubar o usuário e a chave pública registrada.

## Checklist final
- [ ] `LIST @gerolin_fisiovet/sales;` lista arquivos
- [ ] `dbt run --target dev_snowflake` → 10 OK
- [ ] `MAX(date)` de `sales` igual ao do BigQuery
- [ ] Contagens 14 / 84 / 13 nas tabelas de referência
- [ ] DAG diária com `dbt_run_snowflake` verde
- [ ] Metabase lendo o Snowflake novo
- [ ] Grant da SA antiga removido do bucket
