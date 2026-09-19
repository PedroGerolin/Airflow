# Guia de estudo — dbt rodando em BigQuery e Snowflake ao mesmo tempo

## Como o dbt funciona (o essencial)
- Um **model** é um `SELECT` num arquivo `.sql`. O dbt o embrulha num `CREATE TABLE/VIEW AS` (ou
  `MERGE`, nos incrementais) e executa no warehouse.
- Antes de executar, o dbt **compila**: renderiza o Jinja (`{{ ref('sales') }}`,
  `{% if … %}`) e produz SQL puro. O resultado fica em `target/compiled/` e `target/run/` — abra
  esses arquivos pra ver exatamente o que foi mandado ao banco.
- `ref('modelo')` = dependência entre models (define a ordem). `source('grupo','tabela')` = tabela
  que o dbt não cria (as externas). Por isso o `dbt run` mostra os models na ordem certa.

## Um arquivo, dois dialetos
O mesmo `.sql` gera SQL diferente conforme o **target** da execução:
```sql
{% if target.name == 'prod_bigquery' %} FORMAT_DATE('%Y-%m-01', date) {% else %} TO_CHAR(date, 'YYYY-MM-01') {% endif %}
```
O `if` é resolvido **pelo dbt, na compilação** — só o texto do ramo escolhido chega ao banco.
Tabela de equivalências no [guia do Snowflake](snowflake.md#9-sql-específico-do-snowflake-que-aparece-nos-models-do-dbt).

## Configuração: um profile, dois targets
`profiles.yml` (gitignorado; modelo em `profiles.yml.example`) tem o profile `fiosiovet` com os
outputs `prod_bigquery` (padrão) e `dev_snowflake`. `dbt_project.yml` aponta `profile: 'fiosiovet'`.
O `--target` escolhe o output:
```powershell
dbt run --target prod_bigquery ...    # dentro do container (o keyfile é um caminho /opt/airflow/...)
dbt run --target dev_snowflake ...    # pode rodar direto do Windows (.venv)
```
Antes só existiam dois profiles separados (`bigquery:` e `snowflake:`) — e `--target` só escolhe
entre outputs **do mesmo profile**, então não dava pra alternar. Unificar resolveu.

## Hooks
`on-run-start` (no `dbt_project.yml`) executa SQL antes dos models. Usamos pra
`ALTER EXTERNAL TABLE … REFRESH` no Snowflake (tabela externa não enxerga arquivo novo sozinha).
O hook renderiza **vazio** no BigQuery e o dbt o ignora (testado: `OK in 0.01s`).

## Materializações que apareceram
- `table`: recria a cada execução (`create or replace [transient] table`).
- `incremental`: na 1ª vez cria; depois só processa o recorte novo (`is_incremental()`), com
  `unique_key` pra fazer `MERGE`. `sales` reprocessa os últimos 120 dias.
- **transient** (Snowflake): sem Fail-safe e Time Travel curto; padrão do dbt-snowflake.

## Bugs reais que esse projeto teve (leia como exercícios)
1. **`{% if %}` sem `{% else %}` envolvendo o model inteiro** (`resultado_operacional.sql`): fora
   do BigQuery o arquivo compilava **vazio** → erro de sintaxe no Snowflake. Lição: abra o
   `target/run/…` do target que falhou.
2. **Subquery correlacionada no `SELECT`** (`(SELECT x FROM cte WHERE cte.k = t.k)`, 3 vezes): o
   BigQuery aceita, o Snowflake responde "Unsupported subquery type". Trocada por `LEFT JOIN` —
   mais portável e mais eficiente.
3. **Tipagem diferente nas tabelas externas**: BigQuery tipa campos como numéricos e converte
   string vazia em `NULL`; Snowflake dá erro. Corrigido na definição da tabela externa (`NULLIF`).
4. **Snowflake desatualizado em silêncio** (external table sem `REFRESH`): o run passava, os
   dados estavam 2 dias velhos. Sempre comparar `MAX(date)` entre os bancos.
5. **Cache `target/` compartilhado** entre Windows e container → erro `KeyError`; apagar `target/`.
6. **`profiles.yml` apagado** pelo `git filter-repo` (é gitignorado, o filter-repo reseta a
   working tree) → agora existe `profiles.yml.example`.

## Comandos úteis
```powershell
dbt debug  --target dev_snowflake ...   # testa conexão e configuração
dbt run    --target dev_snowflake -s resultado_operacional ...   # só um model
dbt compile ...                          # só gera o SQL em target/, sem executar
```
`dbt test` (validações `unique`/`not_null`) **ainda não roda** neste projeto: só `clients.yml` tem
testes e a DAG não chama `dbt test` — candidato a melhoria.
