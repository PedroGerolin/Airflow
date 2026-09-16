-- Setup Snowflake FisioVet — passo 2: storage integration com o GCS
-- Reaproveitado de scratch pessoal (Fisiovet.txt), versionado em 2026-09-16.

USE ROLE ACCOUNTADMIN;
USE SCHEMA FISIOVET.FISIOVET_EXTERNAL;

CREATE STORAGE INTEGRATION IF NOT EXISTS gerolin_fisiovet
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gerolin_etl/FisioVet/');

GRANT USAGE ON INTEGRATION gerolin_fisiovet TO ROLE SYSADMIN;

-- >>> PASSO MANUAL OBRIGATÓRIO <<<
-- Rode o comando abaixo e copie o valor de STORAGE_GCP_SERVICE_ACCOUNT.
-- Essa identidade é gerada pelo Snowflake e MUDA a cada conta/trial nova.
DESC STORAGE INTEGRATION gerolin_fisiovet;

-- Depois, conceda a essa identidade acesso de leitura ao bucket via gcloud
-- (substitua <SA_GERADA_PELO_SNOWFLAKE> pelo valor obtido acima):
--
--   gcloud storage buckets add-iam-policy-binding gs://gerolin_etl \
--     --member="serviceAccount:<SA_GERADA_PELO_SNOWFLAKE>" \
--     --role="roles/storage.objectViewer"
--
-- (a integration antiga, de uma conta trial anterior, usava
--  squtmsjikk@prod3-f617.iam.gserviceaccount.com com uma custom role chamada
--  "SnowflakeRole" — essa role específica não é necessária, objectViewer basta
--  já que o Snowflake só precisa LER os arquivos, não escrever.)

USE ROLE SYSADMIN;
