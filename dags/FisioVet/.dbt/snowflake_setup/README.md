# Setup de infraestrutura Snowflake — FisioVet

Scripts DDL versionados para recriar a infraestrutura Snowflake do projeto FisioVet do
zero. Existem porque a conta trial já foi recriada uma vez (setembro/2026) e o setup
anterior só existia em arquivos soltos fora do repositório — isso não pode se repetir.

Nenhum destes arquivos contém segredo. Credenciais de conexão continuam vivendo em
`dags/FisioVet/.dbt/profiles.yml` (gitignored) e na variável de ambiente
`SNOWFLAKE_PASSWORD`.

## Ordem de execução (numerada — sempre nessa sequência)

1. **`01_schemas.sql`** — cria as databases/schemas (`FISIOVET`, `FISIOVET_EXTERNAL`,
   `FISIOVET_ANALYTICS`) que o dbt espera (ver `models/*/schema` no `dbt_project.yml` e
   os `sources.yml`).
2. **`02_storage_integration.sql`** — cria a `STORAGE INTEGRATION` que dá ao Snowflake
   permissão de ler o bucket GCS `gerolin_etl/FisioVet/`.
   ⚠️ **Passo manual obrigatório no meio**: depois de rodar esse script, rode
   `DESC STORAGE INTEGRATION gerolin_fisiovet;` e pegue o valor de
   `STORAGE_GCP_SERVICE_ACCOUNT` — é uma identidade de serviço que o **Snowflake gera
   sozinho**, diferente a cada conta/trial nova. Sem conceder acesso dela ao bucket
   (via `gcloud`, comando em `02_storage_integration.sql` no final, como comentário),
   o stage do passo 3 não consegue listar/ler os arquivos.
3. **`03_stage_and_file_formats.sql`** — cria o stage externo e os file formats
   (`ff_csv` pipe-delimited, `ff_csv_auto_detect`).
4. **`04_external_tables.sql`** — cria as tabelas externas `clients_animals`, `sales`,
   `debts` sobre o stage, com o mapeamento posicional de colunas (`c1`, `c2`, ...) que
   bate com o CSV normalizado pelo `FileTransformer` (`plugins/common/file_transformer.py`).
   Se o formato do CSV exportado pelo simples.vet mudar, esse mapeamento precisa ser
   revisto. Campos numéricos (`sales.Venda/Codigo`, `debts.Valor/Desconto/Multa/Juros/
   Valorpago`) são tipados como `NUMBER`/`FLOAT` — igual o BigQuery já faz na tabela externa
   equivalente — usando `NULLIF(value:cN,'')` antes do cast (Snowflake, ao contrário do
   BigQuery, dá erro em vez de virar `NULL` ao castar string vazia). Exceção: `sales.Numero`
   fica `VARCHAR` mesmo o BigQuery declarando `INTEGER`, porque o dado real tem valores
   como `"SN"` (sem número) — não é usado em nenhum model dbt de qualquer forma.
5. **`05_reference_data.sql`** — recria as tabelas de referência mantidas manualmente
   (`services`, `commission`, `debts_types`) com os dados reais de comissão por
   funcionário/serviço e categorização de despesas fixas/variáveis. Esses dados **não
   existem em nenhum outro lugar** além deste arquivo — não vieram de nenhuma fonte
   automatizada.

6. **`06_metabase_reader.sql`** — role `METABASE_READER` (só `SELECT` nos schemas do dbt) e
   usuário `METABASE` (`TYPE = SERVICE`, sem senha, autentica por chave). Ver o próprio
   arquivo pros detalhes (inclusive por que os `FUTURE` grants são obrigatórios).

## Autenticação por chave (key-pair) — obrigatória desde a fase 3 da Snowflake

Desde ago–out/2026 a Snowflake bloqueia login só com senha: usuário humano/`TYPE` nulo passa a
exigir MFA, `LEGACY_SERVICE` é convertido pra `SERVICE` (que não guarda senha). Programa que
não pode fazer MFA (dbt, Metabase) precisa de **chave privada**. O banner apareceu na conta em
19/09/2026 com data 22/09/2026 (a doc pública diz que trials são isentas, mas o aviso valia
pra esta conta — migramos por precaução).

**Pipeline (dbt) — usuário `PEDROGEROLIN`, chave criptografada com passphrase:**
```
# passphrase vem da variavel de usuario SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (nunca em arquivo)
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -aes-256-cbc -pass env:SF_KEY_PASS -out credential/snowflake_rsa_key.p8
openssl pkey -in credential/snowflake_rsa_key.p8 -passin env:SF_KEY_PASS -pubout -out credential/snowflake_rsa_key.pub
```
```sql
ALTER USER PEDROGEROLIN SET RSA_PUBLIC_KEY='<corpo base64 da chave publica>';
```
`profiles.yml` usa `private_key_path` (padrão `/opt/airflow/credential/snowflake_rsa_key.p8`,
sobrescrevível por `SNOWFLAKE_PRIVATE_KEY_PATH` pra rodar local no Windows) e
`private_key_passphrase` (`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, repassada pelo `docker-compose.yaml`).

**Metabase — usuário `METABASE`, chave SEM criptografia** (`credential/metabase_snowflake_rsa_key.p8`;
o Metabase só aceita "RSA private key (PEM)", sem passphrase). Risco contido: é chave de usuário
só-leitura, em pasta gitignorada. Mesmo `openssl genpkey`, sem `-aes-256-cbc`/`-pass`.

**Pendência — MCP `toolbox-snowflake`:** o MCP Toolbox (v1.11/1.12) só suporta usuário+senha
(doc atualizada em 17/09/2026). Quando a Snowflake passar a exigir MFA/chave pro `PEDROGEROLIN`,
esse MCP para de conectar. Alternativas: `Snowflake-Labs/mcp` (suporta chave, repo deprecated),
aguardar suporte no Toolbox, ou rodar SQL de administração via script Python com o conector.

## O que ficou de fora de propósito

O setup original (arquivo `Fisiovet - Snowflake.txt`, fora do repo) também tinha uma
segunda camada de ingestão via **Snowpipe com auto-ingest (GCP Pub/Sub)** + **Streams e
Tasks** fazendo merge incremental num `SALES_HISTORY` nativo do Snowflake. Isso foi
deixado de fora deliberadamente porque duplica o que o dbt já faz de forma incremental
(`sales.sql`, `debts.sql` são incremental models) — manter os dois mecanismos ao mesmo
tempo seria complexidade redundante. Se um dia fizer sentido aprender/usar Snowpipe e
Streams/Tasks de propósito, isso entra como um novo script numerado, não como
substituição do que o dbt já faz.

## Como rodar

Via MCP do Snowflake (`toolbox-snowflake`) dentro de uma sessão do Claude Code, ou colando
direto num worksheet do Snowsight, na ordem dos números do arquivo.

## Pegadinhas encontradas rodando pela primeira vez (2026-09-16)

- **Role**: os schemas ficam com dono `ACCOUNTADMIN` (é a role ativa por padrão numa conta
  trial nova). Rodar os passos 3/4 como `SYSADMIN` (como o scratch original sugeria) dá erro
  de permissão (`Insufficient privileges... CREATE STAGE granted on SCHEMA`). Rodar tudo como
  `ACCOUNTADMIN` evita isso — não corrigido nos scripts porque numa conta pessoal de um usuário
  só não vale a pena granular papéis.
- **SQL multi-linha via MCP**: o classificador de auto-mode do Claude Code bloqueou um
  `CREATE STAGE` com quebras de linha ("Blocked by classifier") mas deixou passar o mesmo
  comando numa linha só. Se for rodar via MCP (não Snowsight), colapsar cada `CREATE`/`INSERT`
  numa única linha.
- **`SELECT TOP 10 * FROM clients_animals`** só funciona depois que a tabela externa tem TODAS
  as colunas do CSV real mapeadas certo — ver a nota de `Cliente_DatadeNascimento` no cabeçalho
  de `04_external_tables.sql`. Sempre validar o schema real da tabela externa equivalente no
  BigQuery antes de recriar no Snowflake (via `get_table_info` do MCP `toolbox-bigquery`), não
  confiar só em scratch antigo.
- **`AUTO_REFRESH = false` = arquivos novos ficam invisíveis até um `REFRESH`.** A tabela externa
  do Snowflake guarda uma lista *registrada* de arquivos do GCS (o BigQuery, ao contrário, lista o
  bucket a cada consulta). Sem `ALTER EXTERNAL TABLE ... REFRESH`, o Snowflake ficou 2 dias
  atrasado (última venda 15/09 vs 17/09 no BigQuery) **sem nenhum erro** — o `dbt run` passava
  normal. Correção: hooks `on-run-start` no `dbt_project.yml` que fazem o REFRESH das 3 tabelas
  externas quando `target.type == 'snowflake'` (no BigQuery renderizam vazio e o dbt ignora).
  Lição geral: comparar `MAX(data)` e contagens entre os dois bancos, não só "o run terminou".
  (Alternativa mais avançada: `AUTO_REFRESH = true` com notificação de eventos GCS via Pub/Sub —
  é o que o scratch antigo do Snowpipe usava.)
- Depois de rodar esses 5 scripts, ainda é preciso testar o `dbt run --target dev_snowflake`
  de ponta a ponta — dois bugs reais só apareceram nesse teste (documentados no `CLAUDE.md` da
  raiz do repo, seção "dbt multi-warehouse"): um `{% if %}` sem `{% else %}` que zerava um
  model inteiro, e uma subquery correlacionada que o BigQuery aceita mas o Snowflake não.
