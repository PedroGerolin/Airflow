-- Setup Snowflake FisioVet — passo 4: tabelas externas
-- Reaproveitado de scratch pessoal (Fisiovet.txt), versionado em 2026-09-16.
-- Mapeamento posicional (c1, c2, ...) alinhado ao CSV normalizado pelo
-- FileTransformer (plugins/common/file_transformer.py). Se o layout do export do
-- simples.vet mudar, este arquivo precisa ser revisado.
--
-- IMPORTANTE: mapeamento de clients_animals CONFERIDO contra o schema real da
-- tabela externa equivalente no BigQuery (FisioVet_External.clients_animals) em
-- 2026-09-16, porque o scratch original estava desatualizado. A produção hoje tem
-- 2 colunas a mais no CSV do que o scratch previa: "Animal_Idade" (posição 8 —
-- já era pulada corretamente no scratch, sem problema) e "Cliente_DatadeNascimento"
-- (posição 21 — essa NÃO estava no scratch e deslocava todo campo a partir de
-- Cliente_Email em diante). Corrigido abaixo.
--
-- Campos numéricos (Venda/Codigo em sales; Valor/Desconto/Multa/Juros/Valorpago em
-- debts) tipados como NUMBER/FLOAT direto na tabela externa, espelhando como o
-- BigQuery já tipa esses mesmos campos na tabela externa equivalente (conferido via
-- get_table_info em 2026-09-16). Evita precisar de CAST/NULLIF condicional por
-- target.name no dbt (models/analytics/resultado_operacional.sql).
-- Exceção: "Numero" (número do endereço) o BigQuery declara como INTEGER, mas o
-- dado real tem valores como "SN" (sem número) — não é numérico de verdade. Mantido
-- como VARCHAR aqui (não é usado em nenhum modelo dbt de qualquer forma).
--
-- IMPORTANTE: diferente do BigQuery (que converte string vazia em NULL sozinho ao
-- carregar CSV num campo numérico), o Snowflake dá erro ("Failed to cast variant
-- value \"\" to REAL") se tentar castar string vazia direto. Por isso os campos
-- numéricos usam NULLIF(value:cN, '') ANTES do cast, dentro da própria definição
-- da external table — não é um workaround no dbt, é equivalente ao comportamento
-- nativo do BigQuery.

USE ROLE SYSADMIN;
USE SCHEMA FISIOVET.FISIOVET_EXTERNAL;

CREATE OR REPLACE EXTERNAL TABLE clients_animals(
    Animal_Codigo integer as (value:c1::number),
    Animal_Nome varchar(200) as (value:c2::varchar),
    Animal_Especie varchar(100) as (value:c3::varchar),
    Animal_Raca varchar(100) as (value:c4::varchar),
    Animal_Pelagem varchar(100) as (value:c5::varchar),
    Animal_Esterilizacao varchar(100) as (value:c6::varchar),
    Animal_Nascimento varchar(10) as (value:c7::varchar),
    Animal_Idade varchar(10) as (value:c8::varchar),
    Animal_Sexo varchar(10) as (value:c9::varchar),
    Animal_Pedigree varchar(50) as (value:c10::varchar),
    Animal_Microchip varchar(50) as (value:c11::varchar),
    Animal_Tags varchar(200) as (value:c12::varchar),
    Animal_Status varchar(50) as (value:c13::varchar),
    Animal_Datadeinclusao varchar(10) as (value:c14::varchar),
    Animal_Vivo_Morto varchar(50) as (value:c15::varchar),
    Cliente_Codigo integer as (value:c16::number),
    Cliente_Nome varchar(200) as (value:c17::varchar),
    Cliente_CPF varchar(15) as (value:c18::varchar),
    Cliente_RG varchar(10) as (value:c19::varchar),
    Cliente_Sexo varchar(10) as (value:c20::varchar),
    Cliente_DatadeNascimento varchar(10) as (value:c21::varchar),
    Cliente_Email varchar(100) as (value:c22::varchar),
    Cliente_Telefones varchar(50) as (value:c23::varchar),
    Cliente_Endereco varchar(400) as (value:c24::varchar),
    Cliente_Bairro varchar(50) as (value:c25::varchar),
    Cliente_Cidade varchar(100) as (value:c26::varchar),
    Cliente_UF varchar(2) as (value:c27::varchar),
    Cliente_CEP varchar(10) as (value:c28::varchar),
    Cliente_Datadeinclusao varchar(10) as (value:c29::varchar),
    Cliente_Datadaultimaatualizacao varchar(10) as (value:c30::varchar),
    Cliente_Origem varchar(100) as (value:c31::varchar),
    Cliente_NPS varchar(10) as (value:c32::varchar),
    Cliente_RankingABC varchar(10) as (value:c33::varchar),
    Cliente_Valorpagonosultimos30dias varchar(10) as (value:c34::varchar),
    Cliente_Valorpagonosultimos90dias varchar(10) as (value:c35::varchar),
    Cliente_Valorpagonosultimos180dias varchar(10) as (value:c36::varchar),
    Cliente_Valorpagonosultimos365dias varchar(10) as (value:c37::varchar),
    Cliente_Ticketmedio varchar(10) as (value:c38::varchar),
    Cliente_Datadaprimeiracompra varchar(10) as (value:c39::varchar),
    Cliente_Ultimavenda varchar(10) as (value:c40::varchar),
    Cliente_UltimoacessoaoSimplesPet varchar(10) as (value:c41::varchar),
    Cliente_Tags varchar(100) as (value:c42::varchar)
)
LOCATION = @gerolin_fisiovet/clients_animals
AUTO_REFRESH = false
FILE_FORMAT = (FORMAT_NAME = ff_csv);

CREATE OR REPLACE EXTERNAL TABLE sales(
    Dataehora VARCHAR(100) AS (value:c1::varchar),
    Venda NUMBER AS (NULLIF(value:c2,'')::number),
    Statusdavenda VARCHAR(100) AS (value:c3::varchar),
    Databaixa VARCHAR(100) AS (value:c4::varchar),
    Formapagamento VARCHAR(100) AS (value:c5::varchar),
    Funcionario VARCHAR(100) AS (value:c6::varchar),
    Cliente VARCHAR(100) AS (value:c7::varchar),
    Codigo NUMBER AS (NULLIF(value:c8,'')::number),
    CPF VARCHAR(100) AS (value:c9::varchar),
    Sexo VARCHAR(100) AS (value:c10::varchar),
    CEP VARCHAR(100) AS (value:c11::varchar),
    Endereco VARCHAR(100) AS (value:c12::varchar),
    Numero VARCHAR(100) AS (value:c13::varchar),
    Bairro VARCHAR(100) AS (value:c14::varchar),
    Email VARCHAR(100) AS (value:c15::varchar),
    Celular VARCHAR(100) AS (value:c16::varchar),
    Animal VARCHAR(100) AS (value:c17::varchar),
    Especie VARCHAR(100) AS (value:c18::varchar),
    Sexo_1 VARCHAR(100) AS (value:c19::varchar),
    Raca VARCHAR(100) AS (value:c20::varchar),
    TipodoItem VARCHAR(100) AS (value:c21::varchar),
    Grupo VARCHAR(100) AS (value:c22::varchar),
    Produto_servico VARCHAR(100) AS (value:c23::varchar),
    ValorUnitario VARCHAR(100) AS (value:c24::varchar),
    Quantidade VARCHAR(100) AS (value:c25::varchar),
    Bruto VARCHAR(100) AS (value:c26::varchar),
    Desconto VARCHAR(100) AS (value:c27::varchar),
    Liquido VARCHAR(100) AS (value:c28::varchar),
    Observacoes VARCHAR(100) AS (value:c29::varchar),
    date DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(metadata$filename,'/',3),'=',2)))
)
PARTITION BY (date)
LOCATION = @gerolin_fisiovet/sales
AUTO_REFRESH = false
FILE_FORMAT = ff_csv;

CREATE OR REPLACE EXTERNAL TABLE debts(
    Data VARCHAR(100) AS (value:c1::varchar),
    Conta VARCHAR(100) AS (value:c2::varchar),
    Categoria VARCHAR(100) AS (value:c3::varchar),
    Descricao VARCHAR(100) AS (value:c4::varchar),
    Fornecedor VARCHAR(100) AS (value:c5::varchar),
    Parcela VARCHAR(100) AS (value:c6::varchar),
    Competencia VARCHAR(100) AS (value:c7::varchar),
    Valor FLOAT AS (NULLIF(value:c8,'')::float),
    Desconto FLOAT AS (NULLIF(value:c9,'')::float),
    Multa FLOAT AS (NULLIF(value:c10,'')::float),
    Juros FLOAT AS (NULLIF(value:c11,'')::float),
    Vencimento VARCHAR(100) AS (value:c12::varchar),
    Pagamento VARCHAR(100) AS (value:c13::varchar),
    Valorpago FLOAT AS (NULLIF(value:c14,'')::float),
    Formadepagamento VARCHAR(100) AS (value:c15::varchar),
    Documento_NF VARCHAR(100) AS (value:c16::varchar),
    Observacao VARCHAR(100) AS (value:c17::varchar),
    date DATE AS (TO_DATE(SPLIT_PART(SPLIT_PART(metadata$filename,'/',3),'=',2)))
)
PARTITION BY (date)
LOCATION = @gerolin_fisiovet/debts
AUTO_REFRESH = false
FILE_FORMAT = ff_csv;

-- Sanity checks
SELECT TOP 10 * FROM FISIOVET.FISIOVET_EXTERNAL.CLIENTS_ANIMALS;
SELECT TOP 10 * FROM FISIOVET.FISIOVET_EXTERNAL.SALES;
SELECT TOP 10 * FROM FISIOVET.FISIOVET_EXTERNAL.DEBTS;
