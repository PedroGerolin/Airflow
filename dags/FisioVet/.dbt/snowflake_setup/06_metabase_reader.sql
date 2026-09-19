-- Setup Snowflake FisioVet — passo 6: usuario/role SOMENTE LEITURA para o Metabase
-- Executado em 2026-09-19. Rodar como ACCOUNTADMIN (dono dos schemas).
-- Pre-requisito: schemas e tabelas do dbt ja existirem (passos 1-5 + um dbt run).
--
-- Principio: o Metabase so precisa LER as tabelas materializadas pelo dbt
-- (FISIOVET e FISIOVET_ANALYTICS). Nao recebe acesso as tabelas externas nem a nada de escrita.

CREATE ROLE IF NOT EXISTS METABASE_READER COMMENT = 'Somente leitura para dashboards no Metabase';

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE METABASE_READER;
GRANT USAGE ON DATABASE FISIOVET TO ROLE METABASE_READER;
GRANT USAGE ON SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;
GRANT USAGE ON SCHEMA FISIOVET.FISIOVET_ANALYTICS TO ROLE METABASE_READER;

GRANT SELECT ON ALL TABLES IN SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA FISIOVET.FISIOVET_ANALYTICS TO ROLE METABASE_READER;

-- FUTURE grants sao essenciais: o dbt recria tabelas com "create or replace", o que apaga
-- grants individuais; o FUTURE reaplica o SELECT automaticamente (validado em 2026-09-19).
GRANT SELECT ON FUTURE TABLES IN SCHEMA FISIOVET.FISIOVET TO ROLE METABASE_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FISIOVET.FISIOVET_ANALYTICS TO ROLE METABASE_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA FISIOVET.FISIOVET_ANALYTICS TO ROLE METABASE_READER;

-- Usuario SEM senha. Desde a fase 3 da Snowflake (ago-out/2026) login so com senha esta sendo
-- bloqueado; TYPE = SERVICE nem armazena senha e autentica por chave (key-pair).
CREATE USER IF NOT EXISTS METABASE
    DEFAULT_ROLE = METABASE_READER
    DEFAULT_WAREHOUSE = COMPUTE_WH
    COMMENT = 'Usuario do Metabase (somente leitura)';
GRANT ROLE METABASE_READER TO USER METABASE;

-- Chave: gerar com openssl (ver README.md, secao "Autenticacao por chave"), SEM criptografia
-- porque o Metabase so aceita "RSA private key (PEM)" sem campo de passphrase. Registrar
-- somente a chave PUBLICA (corpo base64, sem cabecalho/rodape/quebras de linha):
--
--   ALTER USER METABASE SET TYPE = SERVICE RSA_PUBLIC_KEY = '<corpo da chave publica>';
