# Airflow — pipelines FisioVet e WeatherAPI

Projeto Airflow (CeleryExecutor, `apache/airflow:2.10.3-python3.12`) que roda dois pipelines
independentes na mesma instância: **FisioVet** (scraping via Selenium) e **WeatherAPI**
(consumo de API pública). Ambos seguem o padrão Selenium/API → CSV local → GCS → dbt → warehouse.
FisioVet materializa em **BigQuery e Snowflake em paralelo, a cada execução** (não é mais um
target alternativo — os dois bancos ficam sempre espelhados). WeatherAPI usa só BigQuery.

## Estrutura do repositório

```
dags/
  FisioVet/
    fisiovet_dag.py       # DAG principal (produção) — TaskFlow API
    fisiovet_export.py    # variante reduzida, só download (teste manual)
    fisiovet_test.py      # skeleton para testar o `dbt run` isolado
    .dbt/                 # projeto dbt do FisioVet — profile único 'fiosiovet' com dois
                           # targets (prod_bigquery, dev_snowflake)
    .dbt/snowflake_setup/ # scripts DDL versionados p/ recriar a infra Snowflake do zero
  WeatherAPI/
    weather_dag.py         # DAG principal — estilo `with DAG()` clássico + alguns @task
    .dbt/                  # projeto dbt do WeatherAPI (profile: bigquery apenas)
plugins/
  common/     # código reaproveitável entre pipelines: transfer.py, converter.py,
              # exporter.py, file_transformer.py
  fisiovet/   # código específico do FisioVet: fisiovet_downloader.py (Selenium)
  weather/    # código específico do WeatherAPI: weather_hook.py, weather_operator.py
config/requirements.txt   # dependências pip (UTF-16, cuidado ao editar)
credential/                # segredos em arquivo (gitignored): service account GCP, API key
files/                     # landing zone local de CSV/ORC (gitignored)
dev/                       # scripts soltos de teste/scratch, não usados por nenhuma DAG (gitignored)
```

Airflow adiciona `plugins/` ao `sys.path`, então os subpacotes são importados como
`from common.transfer import TransferFile`, `from fisiovet.fisiovet_downloader import ...`,
`from weather.weather_hook import ...` — não usar caminho relativo a `plugins/`.

## Fluxo FisioVet (`fisiovet_dag.py`)

1. `fisiovet_downloader` (task_group) — Selenium loga em `app.simples.vet` (credenciais via
   Airflow Connection `fisioVet`, não hardcoded) e baixa `clientes.csv` e `Vendas.csv`.
2. `file_transformation` — `FileTransformer` normaliza encoding/cabeçalho e separa por data.
3. `file_transfer` — `TransferFile` sobe os CSVs para o bucket GCS `gerolin_etl`.
4. `dbt_run_bigquery` e `dbt_run_snowflake` (dois `BashOperator`, **em paralelo** — ambos saem
   de `file_transfer` e convergem em `end_task`) — materializam camadas `native` (clients,
   animals, sales, debts) e `analytics` (faturamento por cliente/funcionário, resultado
   operacional) em cada warehouse. Por serem independentes (os dois leem do mesmo GCS, não um
   do outro), uma falha num não impede a tentativa no outro.
5. `dbt test` / `dbt source freshness` / `dbt docs generate` estão comentados no código —
   não rodam em produção hoje.

### dbt multi-warehouse (BigQuery + Snowflake)

`profiles.yml` tem **um profile só** (`fiosiovet`, batendo com `profile:` do `dbt_project.yml`)
com dois outputs: `prod_bigquery` (default) e `dev_snowflake`. A DAG passa `--target` explícito
em cada task. Os modelos usam `{% if target.name == 'prod_bigquery' %} ... {% else %} ... {% endif %}`
para alternar sintaxe (`FORMAT_DATE` vs `TO_CHAR`, `FLOAT64` vs `FLOAT`, `SAFE_CAST(...FORMAT...)`
vs `TRY_TO_DATE`/`TO_DATE`).

**As tabelas externas de BigQuery e Snowflake sobre os mesmos CSVs do GCS não são
estruturalmente idênticas em tipagem** — isso já causou bugs reais (setembro/2026):
- BigQuery tipa alguns campos como numérico direto na definição da external table
  (`sales.Venda/Codigo`, `debts.Valor/Desconto/Multa/Juros/Valorpago`); ao recriar o
  equivalente no Snowflake (`dags/FisioVet/.dbt/snowflake_setup/04_external_tables.sql`),
  replicamos essa tipagem — **exceto** `sales.Numero`, que o BigQuery declara `INTEGER` mas
  contém valores como `"SN"` (sem número); mantido `VARCHAR` no Snowflake (não é usado em
  nenhum model dbt de qualquer forma).
- BigQuery converte string vazia em `NULL` automaticamente ao carregar CSV num campo
  numérico; Snowflake **não** — dá erro (`Failed to cast variant value "" to REAL`). Por isso
  os campos numéricos da external table do Snowflake usam `NULLIF(value:cN,'')` antes do cast,
  direto na definição da tabela (não é workaround no dbt).
- Se algum dia recriar essas external tables do zero, siga a ordem numerada em
  `dags/FisioVet/.dbt/snowflake_setup/README.md` — inclui o passo manual de conceder acesso
  GCS à service account que o Snowflake gera (muda a cada conta/trial).

**Gap conhecido**: `fisioVetDownloader.enter_debts_page()` existe mas nenhuma task chama —
o arquivo `contas-a-pagar.csv` que a DAG transforma/transfere precisa ser obtido manualmente.

## Fluxo WeatherAPI (`weather_dag.py`)

`WeatherOperator` busca dados de clima (São Paulo, Calgary, Assis) via `WeatherHook`
(subclasse de `HttpHook`, conexão `weather_api`) → `Converter` converte CSV→ORC →
`TransferFile` sobe pro GCS → dbt cria tabela externa sobre o ORC e agrega em
`weather_summary` → `Exporter` reexporta o resultado do BigQuery pra GCS em parquet.
Schedule semanal (`0 0 * * 1`) com `catchup=True`.

## Convenções e pontos de atenção

- **Estilo de DAG inconsistente**: FisioVet usa TaskFlow API (`@dag`/`@task`/`@task_group`);
  WeatherAPI usa `with DAG()` clássico misturado com `@task`. Ao mexer em uma DAG, manter o
  estilo já usado nela em vez de misturar mais.
- **`TransferFile.transfer_file_gcs()` e `Exporter.export()` chamam `.execute(context=...)`
  diretamente** no operator em vez de retorná-lo como task — bypassa retries/XCom/logging
  nativos do Airflow. Um refactor futuro deveria trocar isso por hooks diretos ou por
  operators de fato encadeados na DAG.
- **Senha do Snowflake nunca em texto puro** — `profiles.yml` usa
  `{{ env_var('SNOWFLAKE_PASSWORD') }}`. A variável precisa existir tanto no host (pro MCP/dbt
  local) quanto no ambiente dos containers Airflow (`docker-compose.yaml` repassa
  `SNOWFLAKE_PASSWORD: ${SNOWFLAKE_PASSWORD:-}` do host pro container — sem isso, `dbt_run_snowflake`
  falha na DAG). Conta/usuário/warehouse/role no `profiles.yml` não são segredo, só a senha.
  Não hardcodar segredos em novos arquivos — usar Airflow Connections (como já é feito para
  `fisioVet` e `weather_api`) ou `env_var()` do dbt.
- **PENDÊNCIA CONHECIDA — `AIRFLOW__CORE__FERNET_KEY` está vazio** no `docker-compose.yaml`.
  Essa chave é o que o Airflow usa pra criptografar senha de Connection/Variable antes de
  guardar no Postgres — com ela vazia, **nada é criptografado**: as senhas de `fisioVet` e
  `weather_api` (cadastradas manualmente pela UI, sem seed no repo) estão em texto puro dentro
  do banco. Corrigir: gerar uma Fernet key real (`Fernet.generate_key()`), passar via variável
  de ambiente (mesmo padrão do `SNOWFLAKE_PASSWORD`, nunca hardcoded no `docker-compose.yaml`
  que é público), e depois reabrir/salvar as Connections existentes na UI pra elas passarem a
  ser criptografadas de fato (trocar a chave sozinho não recriptografa o que já está salvo).
- **`dags/FisioVet/.dbt/profiles.yml` já foi apagado sem querer uma vez** por um
  `git filter-repo` (ele não é rastreado pelo git — é gitignored — e o filter-repo reseta a
  working tree). Se o arquivo sumir do disco, recriar com o conteúdo documentado acima; não é
  um bug do dbt.
- **Testes de dbt praticamente desligados**: só `clients.yml` (FisioVet) tem testes
  `unique`/`not_null` configurados e `dbt test` está comentado na DAG principal.
- Ambos os projetos dbt commitam `profiles.yml` fora do controle de versão
  (`.gitignore`) e não commitam `target/` (build artifact) — manter essa convenção em
  qualquer novo pipeline/projeto dbt adicionado ao repo.

## Execução diária automática (sem manter o Docker ligado o dia todo)

Existe uma tarefa agendada do Windows, `Airflow_FisioVet_Daily` (criada via
`scripts/create_scheduled_task.ps1`, precisa rodar como Administrador), que todo dia às
06:00: sobe o `docker-compose`, dispara a DAG `fisiovet` com um `run_id` previsível
(`scheduled_daily__<timestamp>`), espera terminar (via `airflow tasks states-for-dag-run`,
timeout de 30 min) e derruba os containers de novo — `scripts/run_daily_pipeline.ps1` é
quem faz isso, com log em `logs/daily_pipeline.log`.

A tarefa tem `WakeToRun` habilitado, e os "temporizadores de ativação" do Windows foram
ligados via `powercfg` (`SUB_SLEEP RTCWAKE`, AC e DC) — juntos, isso permite que o
Agendador **acorde o notebook da suspensão** só pra rodar a tarefa. Só funciona a partir de
suspensão (sleep), não de desligado por completo.

**Nem sempre roda às 06:00 em ponto**: o notebook usa Modern Standby e o temporizador de
ativação não acorda a máquina de verdade (nos logs de energia, a saída da suspensão vem sempre
com motivo `Lid` — tampa aberta). Na prática a tarefa roda via `StartWhenAvailable` assim que
o notebook fica disponível (ex: 08:47, 08:52). Aceito pelo usuário. **A tarefa precisa permitir
execução na bateria** (`-AllowStartIfOnBatteries -DontStopIfGoingOnBatteries` no
`create_scheduled_task.ps1`): o padrão do `New-ScheduledTaskSettingsSet` é
`DisallowStartIfOnBatteries=True`, e quando o notebook acordou na bateria (19/09/2026) a
execução foi contada como perdida, sem catch-up, e a próxima só ficou pra 24h depois. Se
mexer nisso, conferir com `(Get-ScheduledTask -TaskName Airflow_FisioVet_Daily).Settings`.

**Cuidado ao editar `run_daily_pipeline.ps1`**: nunca usar `2>&1` ou `2>$null` em chamadas
a `docker`/`docker compose` — no PowerShell 5.1, isso embrulha a saída num `ErrorRecord` e,
com `$ErrorActionPreference = "Stop"`, interrompe o script mesmo quando o comando teve
sucesso (foi exatamente o que quebrou no primeiro teste: o aviso inofensivo
`AIRFLOW_UID not set` do `docker compose up` foi tratado como erro fatal).

## Ambiente de desenvolvimento

- `docker-compose up` sobe Postgres + Redis + webserver + scheduler + worker + triggerer
  (usuário/senha padrão `airflow`/`airflow`, sem `.env` no repo).
- **`SNOWFLAKE_PASSWORD` precisa estar definida como variável de ambiente do Windows** antes
  de subir o compose (`docker-compose up`/`up -d`), senão `dbt_run_snowflake` falha na DAG.
  Definir com `[System.Environment]::SetEnvironmentVariable("SNOWFLAKE_PASSWORD", "...", "User")`
  e reabrir o terminal/VS Code pra pegar a variável nova.
- Rebuild da imagem custom (Chromium para Selenium + `config/requirements.txt`):
  `docker-compose build`.
- Não há `airflow.cfg` no repo — configuração via variáveis de ambiente no
  `docker-compose.yaml`.
- Testar `dbt run` fora da DAG: local via `.venv` (`--profiles-dir`/`--project-dir` apontando
  pra `dags/FisioVet/.dbt`, `--target dev_snowflake` funciona direto do Windows) ou dentro do
  container pro BigQuery (o `keyfile` do `profiles.yml` é um caminho `/opt/airflow/...` que só
  existe lá): `docker compose exec airflow-webserver dbt run --profiles-dir /opt/airflow/dags/FisioVet/.dbt --project-dir /opt/airflow/dags/FisioVet/.dbt`.
  No Git Bash, prefixar com `MSYS_NO_PATHCONV=1` pra esses caminhos `/opt/...` não serem
  reescritos como caminho do Windows.
- Se trocar de target/profile entre execuções locais, `rm -rf dags/FisioVet/.dbt/target` antes
  — o cache de partial-parse do dbt não invalida sozinho e gera erro tipo
  `KeyError: 'dbt_bigquery://macros/adapters.sql'`.
