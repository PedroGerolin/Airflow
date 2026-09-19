# Diário de bordo

Registro cronológico de **tudo que foi feito** no projeto: o quê, por quê, comandos-chave e
conceitos. Detalhes por tecnologia ficam em [`guias/`](guias/). Cada entrada nova segue o modelo
no fim do arquivo. (Entradas de 15–19/09/2026 foram reconstruídas a partir do histórico das sessões.)

---

## 15/09/2026 — Reorganização, segurança do git e MCP do BigQuery
- **Mapeamento do projeto** (Airflow + dbt + Selenium → GCS → BigQuery): 2 pipelines, FisioVet e WeatherAPI.
- **Plugins reorganizados** em `plugins/common` (reutilizável), `plugins/fisiovet`, `plugins/weather`
  (`git mv` preserva histórico; imports das DAGs atualizados). Airflow põe `plugins/` no `sys.path`.
- **`CLAUDE.md` + `.claude/`** criados: contexto do projeto carregado automaticamente em cada sessão.
- **`.gitignore`**: `target/` e `profiles.yml` do dbt do WeatherAPI destrackeados (`git rm --cached`).
- **Validação real**: subir Docker → `airflow dags trigger fisiovet` → `airflow tasks states-for-dag-run`
  (12 tasks `success`). Aprendizado: validar pelo caminho real, não só por `py_compile`.
- **GCP**: `gcloud` instalado (`winget install Google.CloudSDK`), autenticado como owner. SA do ETL
  trocou `roles/editor` por permissões mínimas (guia [gcp-iam](guias/gcp-iam.md)).
- **MCP BigQuery**: o MCP remoto do Google falhou (Claude Code exige Dynamic Client Registration no
  OAuth, o Google não suporta). Solução: `toolbox.exe` (MCP Toolbox) local em modo stdio com a chave
  da SA; registrado em `~/.claude.json` com **caminho completo** (`C:\Users\pedro\bin` só está no PATH do Git Bash).
- **Incidente de segurança**: `profiles.yml` do FisioVet estava rastreado com a senha antiga do
  Snowflake, num repo **público**. Merge dos commits divergentes do GitHub + `git filter-repo` +
  `push --force-with-lease` (guia [git-e-seguranca](guias/git-e-seguranca.md)).
- **MCP Snowflake** (toolbox, `--prebuilt=snowflake`) conectado; senha via `${SNOWFLAKE_PASSWORD}`.

## 16/09/2026 — Snowflake do zero + dbt nos dois bancos + execução diária
- **Infra Snowflake** (scripts `snowflake_setup/01–05`): database/schemas, storage integration,
  stage, file formats, tabelas externas, dados de referência. Guia: [snowflake](guias/snowflake.md).
- Cruzamos o schema com o BigQuery e achamos a **coluna deslocada** (`Cliente_DatadeNascimento`).
- **Tipagem das tabelas externas**: `NULLIF` p/ string vazia; `Numero` ficou `VARCHAR` ("SN").
- **dbt multi-warehouse**: primeiro `dbt run` no Snowflake revelou 2 bugs no `resultado_operacional.sql`
  ([dbt-multiwarehouse](guias/dbt-multiwarehouse.md)). `profiles.yml` unificado (1 profile, 2 targets).
- **DAG**: `dbt_run_bigquery` e `dbt_run_snowflake` **em paralelo**. `docker-compose.yaml` repassa a
  credencial do Snowflake ao container.
- **Execução diária** (`scripts/run_daily_pipeline.ps1` + tarefa agendada): sobe Docker, roda a DAG, derruba.
  ([docker-e-agendamento](guias/docker-e-agendamento.md)).

## 17/09/2026 — Execução diária falha, e o que aprendemos
- A tarefa rodou às 08:52 (não 06:00) e falhou: **Docker Desktop estava fechado** e o erro ia pro
  stderr, que o script não capturava. O script passou a detectar/abrir o Docker Desktop.
- Causa do atraso: **Modern Standby** — o notebook só acordou com a tampa aberta (log de energia:
  motivo `Lid`). Aceito: roda quando o notebook ficar disponível.
- Bug do PowerShell 5.1 (`2>&1`/`*>>` viram `NativeCommandError` com `$ErrorActionPreference="Stop"`).
- **Achado**: `AIRFLOW__CORE__FERNET_KEY` vazio ⇒ senhas de Connections sem criptografia (pendência).
- **BI**: comparativo Metabase × Superset × Power BI; escolhido Metabase self-hosted.

## 18/09/2026 — Validação e Metabase no ar
- Execução diária validada (`RESULTADO: SUCESSO`, 12 tasks).
- **Metabase** em `metabase/docker-compose.yaml`, **compose separado** do Airflow (fica no ar).
  Aprendizado: `healthcheck` em `/api/health`; imagem ~600 MB.

## 19/09/2026 — Bateria, credenciais de leitura, chaves do Snowflake e paridade
- **Execução perdida**: notebook acordou **na bateria** e a tarefa (criada com o padrão do PowerShell,
  `DisallowStartIfOnBatteries=True`) não fez catch-up. Corrigido no `create_scheduled_task.ps1`.
- **Credenciais só-leitura do Metabase** — GCP: SA `metabase-reader` (`dataViewer` só em 2 datasets +
  `jobUser`); Snowflake: role `METABASE_READER` + usuário `METABASE` (script `06`, `FUTURE` grants).
- **Aviso da Snowflake**: login só com senha será bloqueado (22/09/2026). Migração para
  **autenticação por chave (key-pair)** no dbt e no Metabase. Pendência: o MCP `toolbox-snowflake`
  só suporta senha.
- **Bug descoberto ao comparar os bancos**: Snowflake 2 dias atrasado (external table sem `REFRESH`).
  Corrigido com hooks `on-run-start` no dbt.
- **Documentação**: scripts do Snowflake corrigidos (`USE ROLE ACCOUNTADMIN`, o `SYSADMIN` falhava),
  `profiles.yml.example`, `RECRIAR_SNOWFLAKE.md`, esta pasta `docs/`.

---

## Modelo de entrada (copie ao registrar algo novo)
```
## DD/MM/AAAA — Título curto
- **O que**: … (1 linha por mudança)
- **Por quê**: o problema/objetivo
- **Comandos-chave**: `…`  (os completos ficam no script/guia; aqui só os que ensinam algo)
- **Conceito**: o que vale aprender com isso
- **Onde ficou**: caminho do arquivo/script/guia
```
