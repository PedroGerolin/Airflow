# Airflow — pipelines FisioVet e WeatherAPI

Projeto Airflow (CeleryExecutor, `apache/airflow:2.10.3-python3.12`) que roda dois pipelines
independentes na mesma instância: **FisioVet** (scraping via Selenium) e **WeatherAPI**
(consumo de API pública). Ambos seguem o padrão Selenium/API → CSV local → GCS → dbt → BigQuery
(FisioVet também suporta Snowflake como target alternativo do dbt).

## Estrutura do repositório

```
dags/
  FisioVet/
    fisiovet_dag.py       # DAG principal (produção) — TaskFlow API
    fisiovet_export.py    # variante reduzida, só download (teste manual)
    fisiovet_test.py      # skeleton para testar o `dbt run` isolado
    .dbt/                 # projeto dbt do FisioVet (profile: bigquery + snowflake)
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
4. `dbt_run` (BashOperator) — materializa camadas `native` (clients, animals, sales, debts) e
   `analytics` (faturamento por cliente/funcionário, resultado operacional) no BigQuery.
5. `dbt test` / `dbt source freshness` / `dbt docs generate` estão comentados no código —
   não rodam em produção hoje.

O dbt do FisioVet tem dois targets (`prod_bigquery` e `dev_snowflake`) com blocos Jinja
`{% if target.name == ... %}` alternando SQL (BigQuery vs Snowflake). A DAG não passa
`--target`, então roda sempre contra o target padrão do `profiles.yml`.

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
- **Credenciais do Snowflake em texto puro** em `dags/FisioVet/.dbt/profiles.yml` (gitignored,
  não commitado, mas exposto em disco). Não hardcodar segredos em novos arquivos — usar
  Airflow Connections (como já é feito para `fisioVet` e `weather_api`) ou `env_var()` do dbt.
- **Testes de dbt praticamente desligados**: só `clients.yml` (FisioVet) tem testes
  `unique`/`not_null` configurados e `dbt test` está comentado na DAG principal.
- Ambos os projetos dbt commitam `profiles.yml` fora do controle de versão
  (`.gitignore`) e não commitam `target/` (build artifact) — manter essa convenção em
  qualquer novo pipeline/projeto dbt adicionado ao repo.

## Ambiente de desenvolvimento

- `docker-compose up` sobe Postgres + Redis + webserver + scheduler + worker + triggerer
  (usuário/senha padrão `airflow`/`airflow`, sem `.env` no repo).
- Rebuild da imagem custom (Chromium para Selenium + `config/requirements.txt`):
  `docker-compose build`.
- Não há `airflow.cfg` no repo — configuração via variáveis de ambiente no
  `docker-compose.yaml`.
