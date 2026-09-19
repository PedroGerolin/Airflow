# Guia de estudo — Snowflake no projeto FisioVet

Tudo o que foi feito no Snowflake, com os comandos e o porquê. Quem executou cada um:
**[você]** no Snowsight · **[Claude]** via MCP · **[dbt]** dentro do `dbt run`.
Os scripts versionados (fonte da verdade pra refazer) ficam em
`dags/FisioVet/.dbt/snowflake_setup/` (01–06). Pra recriar tudo numa conta nova:
[`docs/RECRIAR_SNOWFLAKE.md`](../RECRIAR_SNOWFLAKE.md).

## Mapa geral

```
GCS (CSVs crus) ─ Storage Integration ─ Stage ─ File Format ─► External Table ─► dbt ─► Tabelas nativas ─► Metabase
   (autentica)        (ponteiro)      (como ler)   (visão viva dos arquivos)  (transforma)  (dado guardado)    (só lê)
```
Hierarquia: **conta → database (`FISIOVET`) → schema (`FISIOVET_EXTERNAL`, `FISIOVET`,
`FISIOVET_ANALYTICS`) → tabelas / stages / file formats**.

## 1. Contexto: onde estou e como
```sql
SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();  -- [Claude]
SHOW WAREHOUSES;   -- COMPUTE_WH: X-Small, AUTO_SUSPEND=300s, AUTO_RESUME=true
```
- `CURRENT_ACCOUNT()` devolve o *locator* da conta (um código curto tipo `ABC12345`), mas a
  conexão usa o identificador `ORGANIZACAO-CONTA`: dois jeitos de identificar a mesma conta.
- **Warehouse** = o "computador" que executa queries. Só consome crédito enquanto roda (suspende
  sozinho após 300 s). Armazenamento e computação são cobrados separadamente — ideia central da
  arquitetura do Snowflake.
- Pra consultar é preciso ter uma **role** ativa; `USE ROLE` / `USE SCHEMA` mudam o contexto.

## 2. Database e schemas (`01_schemas.sql`)
```sql
CREATE DATABASE IF NOT EXISTS FISIOVET;
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET_EXTERNAL;    -- "porta de entrada": dado cru do GCS
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET;             -- native: onde o dbt materializa
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET_ANALYTICS;   -- analytics
DROP SCHEMA IF EXISTS FISIOVET.PUBLIC;                     -- schema padrão que não usamos
```
Separar por camadas (raw → staging → marts) é a organização típica de um data warehouse.

## 3. Ligando o Snowflake ao GCS (`02` e `03`)
```sql
USE ROLE ACCOUNTADMIN;
-- (a) INTEGRATION: a "ponte de autenticação". Não guarda chave nenhuma.
CREATE STORAGE INTEGRATION IF NOT EXISTS gerolin_fisiovet
  TYPE = EXTERNAL_STAGE  STORAGE_PROVIDER = 'GCS'  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gerolin_etl/FisioVet/');   -- só esse prefixo é permitido
GRANT USAGE ON INTEGRATION gerolin_fisiovet TO ROLE SYSADMIN;
DESC STORAGE INTEGRATION gerolin_fisiovet;   -- mostra STORAGE_GCP_SERVICE_ACCOUNT
```
O `DESC` é o passo-chave: o Snowflake **gera uma identidade própria no Google** (uma service
account, ex.: `kzao50000@prod3-f617...`), e é ela que você autoriza no bucket com `gcloud`
(ver `gcp_setup/README.md`). Dupla trava: a integration só permite o prefixo configurado **e**
o bucket precisa ter concedido leitura à identidade.
```sql
-- (b) STAGE: ponteiro nomeado pra uma pasta (não copia nada)
CREATE STAGE IF NOT EXISTS gerolin_fisiovet URL='gcs://gerolin_etl/FisioVet/'
  DIRECTORY=(ENABLE=true) STORAGE_INTEGRATION=gerolin_fisiovet;
LIST @gerolin_fisiovet/clients_animals;      -- prova de que a permissão funcionou
-- (c) FILE FORMAT: receita de como interpretar o arquivo
CREATE OR REPLACE FILE FORMAT ff_csv TYPE='CSV' SKIP_HEADER=1
  FIELD_OPTIONALLY_ENCLOSED_BY='"' FIELD_DELIMITER='|';
```
`ff_csv_auto_detect` (com `PARSE_HEADER=TRUE`) existe só pra `INFER_SCHEMA`, que descobre as
colunas sozinho.

## 4. Tabelas externas (`04_external_tables.sql`)
```sql
CREATE OR REPLACE EXTERNAL TABLE sales(
    Dataehora VARCHAR(100) AS (value:c1::varchar),
    Venda     NUMBER       AS (NULLIF(value:c2,'')::number),
    ...
    date DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(metadata$filename,'/',3),'=',2)))
)
PARTITION BY (date)  LOCATION=@gerolin_fisiovet/sales  AUTO_REFRESH=false  FILE_FORMAT=ff_csv;
```
- Cada linha vira uma coluna **`VALUE` (tipo VARIANT)**; num CSV os campos são `c1, c2, c3…`
  **por posição**. Cada coluna da tabela é uma *expressão* sobre `VALUE`.
- `metadata$filename` é uma pseudo-coluna com o caminho do arquivo. Extraímos a data da pasta
  `date=2025-06-01`. `PARTITION BY (date)` deixa o Snowflake pular arquivos fora do filtro.
- **Nada é armazenado**: cada `SELECT` lê os arquivos do GCS.

**Bugs que essa parte ensinou (ótimos exercícios):**
1. **Posição deslocada** — o CSV real tinha 42 colunas, o scratch antigo 41; sem checar, o e-mail
   viria no lugar do telefone. Validado com `SELECT Cliente_Email, Cliente_Telefones …`.
   Lição: comparar com o schema real da outra fonte (aqui, a tabela externa do BigQuery).
2. `Failed to cast variant value "" to REAL` — o Snowflake **não** converte string vazia em NULL
   (o BigQuery converte). Solução: `NULLIF(value:c14,'')::float`.
3. `Failed to cast variant value "SN" to FIXED` — `Numero` (endereço) tem "SN" (sem número);
   ficou `VARCHAR`.
4. **`AUTO_REFRESH = false`** — a tabela guarda uma *lista registrada* de arquivos; arquivo novo
   só aparece após `ALTER EXTERNAL TABLE … REFRESH`. O Snowflake ficou 2 dias atrasado
   **sem erro nenhum**. Hoje o dbt faz o REFRESH nos hooks `on-run-start`
   (`dbt_project.yml`). Lição: comparar `MAX(data)` entre os bancos, não só "o run terminou".

Dica: `COUNT(*)` conta linhas; `COUNT(col)` **ignora NULLs** (usado pra achar as dívidas ainda
não pagas: 195 de 226 tinham valor pago).

## 5. Tabelas nativas (`05_reference_data.sql` e as do dbt)
```sql
CREATE TABLE IF NOT EXISTS COMMISSION(Funcionario STRING, ProdutoServico STRING, Comissao NUMERIC(8,2));
INSERT INTO COMMISSION (Funcionario, ProdutoServico, Comissao) VALUES ('…','…','0'), …;   -- 84 linhas
```
`SERVICES` (14) e `DEBTS_TYPES` (13) idem — dado guardado **dentro** do Snowflake.
Depois vêm as que o **[dbt]** cria, por exemplo:
```sql
create or replace transient table FISIOVET.FISIOVET_Analytics.resultado_operacional as ( SELECT … )
```
1. `create or replace` recria a tabela e **apaga os grants individuais** dela.
2. dbt-snowflake cria tabelas **transient** por padrão (Time Travel de no máx. 1 dia, sem
   Fail-safe, mais baratas); `sales` usa `transient=false`.
3. Modelos incrementais só reprocessam um recorte (por isso `sales` mostra ~1.250 linhas: janela
   de 120 dias).

## 6. Segurança: roles, grants, usuários (`06_metabase_reader.sql`)
```sql
CREATE ROLE IF NOT EXISTS METABASE_READER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE METABASE_READER;
GRANT USAGE ON DATABASE FISIOVET TO ROLE METABASE_READER;
GRANT USAGE ON SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;              -- e FISIOVET_ANALYTICS
GRANT SELECT ON ALL TABLES    IN SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;  -- as que existem hoje
GRANT SELECT ON FUTURE TABLES IN SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;  -- as que o dbt criar
CREATE USER IF NOT EXISTS METABASE DEFAULT_ROLE=METABASE_READER DEFAULT_WAREHOUSE=COMPUTE_WH;
GRANT ROLE METABASE_READER TO USER METABASE;
SHOW GRANTS TO ROLE METABASE_READER;
```
- **Privilégios vão pra roles; roles vão pra usuários.** Ler uma tabela exige `USAGE` no
  database **e** no schema **e** `SELECT` na tabela — faltando qualquer um, dá erro.
- **`FUTURE` grants** são essenciais aqui: como o `create or replace` do dbt apaga grants, sem
  eles o Metabase perderia acesso a cada execução (validado com `animals`/`clients`/
  `resultado_operacional`, que foram recriadas).
- **Ownership**: `SYSADMIN` falhou em `CREATE STAGE` porque os schemas eram de `ACCOUNTADMIN`.
  Quem cria é dono. Numa empresa real ninguém usa `ACCOUNTADMIN` no dia a dia; aqui simplificamos
  por ser conta pessoal — por isso todos os scripts usam `ACCOUNTADMIN`.
- Roles de sistema: `ACCOUNTADMIN` (tudo) › `SECURITYADMIN`/`USERADMIN` (usuários e grants) ›
  `SYSADMIN` (objetos) › `PUBLIC`.

## 7. Autenticação por chave (key-pair)
```sql
ALTER USER PEDROGEROLIN SET RSA_PUBLIC_KEY='MIIBIjAN…';                     -- só a chave PÚBLICA
ALTER USER METABASE SET TYPE = SERVICE RSA_PUBLIC_KEY='MIIBIjAN…';
SHOW USERS LIKE 'METABASE';   -- has_password=false, has_rsa_public_key=true, type=SERVICE
```
Você guarda a **privada** (`credential/*.p8`, gitignorada), o Snowflake guarda a **pública** e
valida uma assinatura — nenhuma senha trafega. `TYPE=SERVICE` = usuário de programa: não pode ter
senha nem MFA, só chave/token. Motivo: a Snowflake está bloqueando login só com senha (fase 3,
ago–out/2026). Geração das chaves: `snowflake_setup/README.md`.

## 8. Comandos de diagnóstico que valem decorar
`SHOW USERS / ROLES / GRANTS / WAREHOUSES / TABLES` · `DESC STORAGE INTEGRATION` ·
`LIST @stage` · `SELECT … FROM DIRECTORY(@stage)` · `<db>.INFORMATION_SCHEMA.TABLES` /
`TABLE_PRIVILEGES`.

## 9. SQL específico do Snowflake que aparece nos models do dbt
| Necessidade | Snowflake | BigQuery |
|---|---|---|
| Formatar data `YYYY-MM-01` | `TO_CHAR(date,'YYYY-MM-01')` | `FORMAT_DATE('%Y-%m-01', date)` |
| Tipo decimal | `FLOAT` | `FLOAT64` |
| Data BR tolerante a erro | `TRY_TO_DATE(x,'DD/MM/YYYY')` | `SAFE_CAST(x AS DATE FORMAT 'DD/MM/YYYY')` |
| Data+hora BR | `TO_DATE(x,'DD/MM/YYYY HH24:MI')` | `CAST(x AS DATETIME FORMAT …)` |
| Nulo → valor | `IFNULL(x,0)` | `IFNULL(x,0)` |
| Deduplicar | `QUALIFY ROW_NUMBER() OVER(…) = 1` | idem |

## 10. Pra praticar no Snowsight
1. `SELECT $1 FROM @gerolin_fisiovet/sales (FILE_FORMAT => 'ff_csv') LIMIT 5;` — lê arquivo direto do stage.
2. `SELECT * FROM TABLE(INFER_SCHEMA(LOCATION=>'@gerolin_fisiovet/debts', FILE_FORMAT=>'ff_csv_auto_detect'));`
3. **Time Travel**: `SELECT * FROM FISIOVET.FISIOVET.SALES AT(OFFSET => -3600) LIMIT 5;`
4. **Zero-copy clone**: `CREATE TABLE FISIOVET.FISIOVET.SALES_TESTE CLONE FISIOVET.FISIOVET.SALES;`
5. `SELECT GET_DDL('TABLE','FISIOVET.FISIOVET.SALES');`
6. Desafio: crie um usuário de teste com role própria e veja o que ele consegue/não consegue ler.

## 11. Ficou de fora de propósito
**Snowpipe, Streams e Tasks** (ingestão automática dentro do Snowflake, sem dbt) — planejado como
"versão 3", ver [`snowflake-v3-snowpipe.md`](snowflake-v3-snowpipe.md).
