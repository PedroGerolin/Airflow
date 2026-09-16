-- Setup Snowflake FisioVet — passo 1: databases e schemas
-- Reaproveitado de scratch pessoal (Fisiovet.txt), versionado em 2026-09-16.
-- Idempotente: seguro rodar de novo numa conta já configurada.

CREATE DATABASE IF NOT EXISTS FISIOVET;

USE DATABASE FISIOVET;

-- Camada "native"/staging: tabelas externas em cima dos CSVs crus do GCS
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET_EXTERNAL;

-- Camada onde o dbt materializa os modelos "native" (clients, animals, sales, debts)
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET;

-- Camada onde o dbt materializa os modelos "analytics" (faturamento, resultado operacional)
CREATE SCHEMA IF NOT EXISTS FISIOVET.FISIOVET_ANALYTICS;

-- Schema padrão do Snowflake, não usado neste projeto
DROP SCHEMA IF EXISTS FISIOVET.PUBLIC;
