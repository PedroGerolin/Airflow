# Versão 3 (futura) — ingestão nativa no Snowflake com Snowpipe, Streams e Tasks

> **Status: NÃO executado nesta conta.** É o plano de estudo/implementação, baseado no seu
> material legado (`OneDrive\Estudos\Snowflake\Fisiovet - Snowflake.txt`) na conta trial antiga.
> Aquele arquivo também contém a tabela de comissões, então **não foi copiado pro repo público**;
> os trechos abaixo são só a parte de ingestão, sem dados de negócio.

## As três versões do mesmo dado

| | **V1 — hoje (BigQuery)** | **V2 — hoje (Snowflake)** | **V3 — futura (Snowflake nativo)** |
|---|---|---|---|
| Lê do GCS via | tabela externa | tabela externa (+ `REFRESH`) | **Snowpipe** (`COPY INTO` automático) |
| Transforma com | dbt | dbt | **Streams + Tasks (`MERGE`)** |
| Quem dispara | DAG (`dbt run`) | DAG (`dbt run`) | **evento do GCS** (arquivo novo) |
| Onde o dado "vive" | tabela nativa do dbt | tabela nativa do dbt | tabelas `*_PIPE` + `*_HISTORY` |

O valor de fazer a V3 é **aprender os recursos nativos** (SnowPro Core cobre todos) e comparar
com a abordagem dbt: custo, latência, manutenção, testabilidade.

## Conceitos

- **Snowpipe**: `COPY INTO` que roda sozinho, *serverless*, quando um arquivo chega ao stage.
  Guarda o histórico de arquivos já carregados (não duplica).
- **Notification integration + Pub/Sub**: no GCS, quem avisa "chegou arquivo" é um tópico
  **Pub/Sub**; o Snowflake assina uma *subscription* e dispara o pipe.
- **Stream**: "câmera" sobre uma tabela — expõe só as linhas **novas/alteradas** desde a última
  leitura (captura de mudanças, CDC).
- **Task**: SQL agendado (`SCHEDULE`) e/ou condicionado (`WHEN system$stream_has_data(...)`).
- **`MERGE`**: upsert (insere se não existe, atualiza se existe) — faz o papel do `incremental`
  do dbt.

## Sequência (legado, comentada) — para reexecutar numa conta nova

```sql
-- 1) Tabelas "pipe": onde o Snowpipe despeja o dado cru (tudo VARCHAR, como no CSV)
CREATE OR REPLACE TABLE FISIOVET.FISIOVET_EXTERNAL.SALES_PIPE ( Dataehora VARCHAR(200), Venda VARCHAR(200), ... );

-- 2) GCP: tópico Pub/Sub que avisa quando chega arquivo no bucket
--    gsutil notification create -t FISIOVET -f json -e OBJECT_FINALIZE gs://gerolin_etl
--    (e criar a subscription 'fisiovet_subs' nesse tópico)

-- 3) Snowflake: integração de notificação apontando pra subscription
USE ROLE ACCOUNTADMIN;
CREATE NOTIFICATION INTEGRATION INT_FISIOVET TYPE = QUEUE NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/gerolingcp/subscriptions/fisiovet_subs';
DESC NOTIFICATION INTEGRATION INT_FISIOVET;   -- devolve uma service account: dar a ela papel
                                              -- "Pub/Sub Subscriber" na subscription (como na storage integration)

-- 4) O pipe: COPY INTO automático a cada arquivo novo
CREATE OR REPLACE PIPE PIPE_GET_SALES AUTO_INGEST = true INTEGRATION = INT_FISIOVET AS
  COPY INTO FISIOVET.FISIOVET_EXTERNAL.SALES_PIPE
  FROM @gerolin_fisiovet/sales FILE_FORMAT = (FORMAT_NAME = ff_csv);
ALTER PIPE PIPE_GET_SALES REFRESH;   -- carrega o que já estava no stage antes do pipe existir

-- 5) Stream: captura só o que entrou na SALES_PIPE
CREATE OR REPLACE STREAM FISIOVET.FISIOVET_EXTERNAL.ST_SALES ON TABLE FISIOVET.FISIOVET_EXTERNAL.SALES_PIPE;
SELECT SYSTEM$STREAM_HAS_DATA('ST_SALES');

-- 6) Task: quando o stream tiver dado, faz MERGE na tabela final (SALES_HISTORY, com tipos corretos)
CREATE OR REPLACE TASK FISIOVET.FISIOVET_EXTERNAL.TSK_SALES_HISTORY
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'  SCHEDULE = '12 HOURS'
  WHEN SYSTEM$STREAM_HAS_DATA('ST_SALES')
AS MERGE INTO FISIOVET.FISIOVET.SALES_HISTORY SH USING ( SELECT … casts BR … FROM ST_SALES ) S
   ON SH.VENDA = S.VENDA AND SH.CODIGOCLIENTE = S.CODIGO AND SH.NOMEANIMAL = S.ANIMAL
      AND SH.PRODUTOSERVICO = S.PRODUTO_SERVICO
   WHEN NOT MATCHED THEN INSERT (…) VALUES (…)
   WHEN MATCHED THEN UPDATE SET …;
ALTER TASK FISIOVET.FISIOVET_EXTERNAL.TSK_SALES_HISTORY RESUME;   -- ver armadilha 1
```

## Armadilhas já conhecidas (leia antes de refazer)
1. **Task nasce suspensa**: o legado não tinha `ALTER TASK … RESUME`, então nada rodava.
2. `PIPE_EXECUTION_PAUSED = true/false` pausa/retoma o pipe (o legado alternava sem anotar o estado).
3. O `SHOW`/`DESC NOTIFICATION INTEGRATION` do legado tinha um typo (`INT_SALES`); o nome é `INT_FISIOVET`.
4. Nova conta ⇒ **novas identidades geradas** (storage integration e notification integration):
   refazer os grants no GCP (bucket `objectViewer` + Pub/Sub `subscriber`).
5. A chave de negócio do `MERGE` (venda + cliente + animal + produto) precisa ser única, senão
   o `MERGE` falha por "duplicate row".
6. Custo: Snowpipe e Tasks consomem crédito (serverless / warehouse) — monitore com
   `SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY` e `TASK_HISTORY`.

## Como encaixar no projeto sem quebrar o que existe
Criar um schema separado (ex.: `FISIOVET_V3`) pra não colidir com as tabelas do dbt, e comparar
`COUNT(*)`, `MAX(date)` e somas entre V2 e V3 (a mesma checagem de paridade que já usamos entre
BigQuery e Snowflake). Cada script novo entra em `snowflake_setup/` numerado (07, 08, …) e cada
comando de GCP em `gcp_setup/README.md`, como o resto.
