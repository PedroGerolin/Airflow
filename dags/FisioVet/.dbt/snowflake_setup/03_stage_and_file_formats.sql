-- Setup Snowflake FisioVet — passo 3: stage externo e file formats
-- Reaproveitado de scratch pessoal (Fisiovet.txt), versionado em 2026-09-16.
-- Pré-requisito: 02_storage_integration.sql já rodado, incluindo o grant de IAM no GCP.

-- ACCOUNTADMIN (e nao SYSADMIN): os schemas foram criados por ACCOUNTADMIN, que e o dono.
-- Rodar como SYSADMIN falha com 'Insufficient privileges ... CREATE STAGE/TABLE granted on SCHEMA'.
USE ROLE ACCOUNTADMIN;
USE SCHEMA FISIOVET.FISIOVET_EXTERNAL;

CREATE STAGE IF NOT EXISTS gerolin_fisiovet
    URL = 'gcs://gerolin_etl/FisioVet/'
    DIRECTORY = ( ENABLE = true )
    STORAGE_INTEGRATION = gerolin_fisiovet;

-- Formato usado pelos CSVs já normalizados pelo FileTransformer (pipe-delimited,
-- primeira linha é header e é ignorada porque as tabelas externas usam colunas
-- posicionais c1, c2, ...)
CREATE OR REPLACE FILE FORMAT ff_csv
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = '|';

-- Variante usada só para inferir schema (INFER_SCHEMA), não para as tabelas externas
-- de produção.
CREATE OR REPLACE FILE FORMAT ff_csv_auto_detect
    TYPE = 'CSV'
    PARSE_HEADER = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = '|';

-- Sanity check: deve listar os arquivos que o Airflow já subiu pro GCS
LIST @gerolin_fisiovet/clients_animals;
LIST @gerolin_fisiovet/sales;
LIST @gerolin_fisiovet/debts;
