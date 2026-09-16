-- Setup Snowflake FisioVet — passo 5: tabelas de referência mantidas manualmente
-- Reaproveitado de scratch pessoal (Fisiovet.txt), versionado em 2026-09-16.
-- Estes dados (comissão por funcionário/serviço, categorização de despesas) não vêm
-- de nenhuma fonte automatizada — são mantidos manualmente e só existiam nesse
-- arquivo de scratch antes desta migração para o repositório.

USE ROLE SYSADMIN;
USE SCHEMA FISIOVET.FISIOVET;

CREATE TABLE IF NOT EXISTS SERVICES(
    Servico STRING,
    Categoria STRING,
    Local STRING
);

DELETE FROM SERVICES;
INSERT INTO SERVICES (Servico, Categoria, Local)
VALUES
 ('Hidroterapia','Fisioterapia','Clínica')
,('Librela','Medicação','Clínica')
,('Fisioterapia - Sessão (Pacote 14 sessões)','Fisioterapia','Clínica')
,('Acupuntura - Consulta (Clínica)','Acupuntura','Clínica')
,('Acupuntura - Sessão (Clínica)','Acupuntura','Clínica')
,('Treino Neurolocomotor','Fisioterapia','Clínica')
,('Fisioterapia - Sessão (Pacote 6 sessões)','Fisioterapia','Clínica')
,('Fisioterapia - Sessão (Clínica)','Fisioterapia','Clínica')
,('Fisioterapia - Consulta (Clínica)','Fisioterapia','Clínica')
,('Fisioterapia - Sessão (Pacote 10 sessões)','Fisioterapia','Clínica')
,('Acupuntura - Sessão (Domicílio)','Acupuntura','Domicílio')
,('Fisioterapia - Consulta (Domicílio)','Fisioterapia','Domicílio')
,('Acupuntura - Consulta (Domicílio)','Acupuntura','Domicílio')
,('Fisioterapia - Sessão (Domicílio)','Fisioterapia','Domicílio');

CREATE TABLE IF NOT EXISTS COMMISSION(
  Funcionario STRING,
  ProdutoServico STRING,
  Comissao NUMERIC(8, 2)
);

DELETE FROM COMMISSION;
INSERT INTO COMMISSION (Funcionario, ProdutoServico, Comissao)
VALUES
 ('Beatriz Feijó','Librela','0')
,('Beatriz Feijó','Fisioterapia - Consulta (Domicílio)','55')
,('Beatriz Feijó','Fisioterapia - Sessão (Domicílio)','55')
,('Beatriz Feijó','Acupuntura - Sessão (Clínica)','50')
,('Beatriz Feijó','Acupuntura - Sessão (Domicílio)','50')
,('Beatriz Feijó','Acupuntura - Consulta (Domicílio)','50')
,('Beatriz Feijó','Acupuntura - Consulta (Clínica)','50')
,('Beatriz Feijó','Treino Neurolocomotor','37.5')
,('Beatriz Feijó','Fisioterapia - Sessão (Pacote 6 sessões)','37.5')
,('Beatriz Feijó','Fisioterapia - Consulta (Clínica)','37.5')
,('Beatriz Feijó','Fisioterapia - Sessão (Pacote 14 sessões)','37.5')
,('Beatriz Feijó','Fisioterapia - Sessão (Clínica)','37.5')
,('Beatriz Feijó','Hidroterapia','37.5')
,('Beatriz Feijó','Fisioterapia - Sessão (Pacote 10 sessões)','37.5')
,('Debora Turini','Librela','0')
,('Debora Turini','Fisioterapia - Consulta (Domicílio)','65')
,('Debora Turini','Acupuntura - Sessão (Domicílio)','65')
,('Debora Turini','Fisioterapia - Sessão (Domicílio)','65')
,('Debora Turini','Fisioterapia - Sessão (Clínica)','55')
,('Debora Turini','Fisioterapia - Sessão (Pacote 6 sessões)','55')
,('Debora Turini','Hidroterapia','55')
,('Debora Turini','Fisioterapia - Sessão (Pacote 14 sessões)','55')
,('Debora Turini','Acupuntura - Sessão (Clínica)','55')
,('Debora Turini','Fisioterapia - Sessão (Pacote 10 sessões)','55')
,('Debora Turini','Fisioterapia - Consulta (Clínica)','55')
,('Debora Turini','Treino Neurolocomotor','55')
,('Debora Turini','Acupuntura - Consulta (Domicílio)','50')
,('Debora Turini','Acupuntura - Consulta (Clínica)','50')
,('Gislaine Pinto','Librela','0')
,('Gislaine Pinto','Fisioterapia - Sessão (Domicílio)','55')
,('Gislaine Pinto','Fisioterapia - Consulta (Domicílio)','55')
,('Gislaine Pinto','Acupuntura - Consulta (Domicílio)','50')
,('Gislaine Pinto','Acupuntura - Sessão (Domicílio)','50')
,('Gislaine Pinto','Acupuntura - Sessão (Clínica)','50')
,('Gislaine Pinto','Acupuntura - Consulta (Clínica)','50')
,('Gislaine Pinto','Hidroterapia','35')
,('Gislaine Pinto','Fisioterapia - Sessão (Pacote 14 sessões)','35')
,('Gislaine Pinto','Treino Neurolocomotor','35')
,('Gislaine Pinto','Fisioterapia - Sessão (Pacote 6 sessões)','35')
,('Gislaine Pinto','Fisioterapia - Consulta (Clínica)','35')
,('Gislaine Pinto','Fisioterapia - Sessão (Pacote 10 sessões)','35')
,('Gislaine Pinto','Fisioterapia - Sessão (Clínica)','35')
,('Karina Kiataqui','Librela','0')
,('Karina Kiataqui','Acupuntura - Sessão (Clínica)','50')
,('Karina Kiataqui','Fisioterapia - Sessão (Domicílio)','50')
,('Karina Kiataqui','Acupuntura - Consulta (Clínica)','50')
,('Karina Kiataqui','Fisioterapia - Consulta (Domicílio)','50')
,('Karina Kiataqui','Acupuntura - Consulta (Domicílio)','50')
,('Karina Kiataqui','Acupuntura - Sessão (Domicílio)','50')
,('Karina Kiataqui','Fisioterapia - Consulta (Clínica)','35')
,('Karina Kiataqui','Fisioterapia - Sessão (Clínica)','35')
,('Karina Kiataqui','Fisioterapia - Sessão (Pacote 6 sessões)','35')
,('Karina Kiataqui','Hidroterapia','35')
,('Karina Kiataqui','Fisioterapia - Sessão (Pacote 10 sessões)','35')
,('Karina Kiataqui','Treino Neurolocomotor','35')
,('Karina Kiataqui','Fisioterapia - Sessão (Pacote 14 sessões)','35')
,('Laura Ferreira','Librela','0')
,('Laura Ferreira','Fisioterapia - Sessão (Domicílio)','50')
,('Laura Ferreira','Acupuntura - Consulta (Clínica)','50')
,('Laura Ferreira','Acupuntura - Sessão (Domicílio)','50')
,('Laura Ferreira','Acupuntura - Sessão (Clínica)','50')
,('Laura Ferreira','Fisioterapia - Consulta (Domicílio)','50')
,('Laura Ferreira','Acupuntura - Consulta (Domicílio)','50')
,('Laura Ferreira','Hidroterapia','35')
,('Laura Ferreira','Treino Neurolocomotor','35')
,('Laura Ferreira','Fisioterapia - Sessão (Pacote 10 sessões)','35')
,('Laura Ferreira','Fisioterapia - Sessão (Pacote 6 sessões)','35')
,('Laura Ferreira','Fisioterapia - Sessão (Pacote 14 sessões)','35')
,('Laura Ferreira','Fisioterapia - Sessão (Clínica)','35')
,('Laura Ferreira','Fisioterapia - Consulta (Clínica)','35')
,('Miriam Prado','Librela','0')
,('Miriam Prado','Acupuntura - Consulta (Clínica)','50')
,('Miriam Prado','Fisioterapia - Consulta (Domicílio)','50')
,('Miriam Prado','Fisioterapia - Sessão (Domicílio)','50')
,('Miriam Prado','Acupuntura - Consulta (Domicílio)','50')
,('Miriam Prado','Acupuntura - Sessão (Domicílio)','50')
,('Miriam Prado','Acupuntura - Sessão (Clínica)','50')
,('Miriam Prado','Fisioterapia - Sessão (Pacote 10 sessões)','35')
,('Miriam Prado','Fisioterapia - Sessão (Clínica)','35')
,('Miriam Prado','Fisioterapia - Consulta (Clínica)','35')
,('Miriam Prado','Hidroterapia','35')
,('Miriam Prado','Fisioterapia - Sessão (Pacote 6 sessões)','35')
,('Miriam Prado','Fisioterapia - Sessão (Pacote 14 sessões)','35')
,('Miriam Prado','Treino Neurolocomotor','35');

CREATE TABLE IF NOT EXISTS DEBTS_TYPES(
  CategoriaDebito STRING,
  Local STRING,
  Tipo STRING
);

DELETE FROM DEBTS_TYPES;
INSERT INTO DEBTS_TYPES (CategoriaDebito, Local, Tipo)
VALUES
 ('Água','Clínica','Fixo')
,('Profissionais terceirizados','Clínica','Fixo')
,('Energia elétrica','Clínica','Fixo')
,('Cursos e treinamentos','Clínica','Fixo')
,('Aluguel','Clínica','Fixo')
,('Material de Escritório','Clínica','Fixo')
,('Devoluções','Clínica','Fixo')
,('Supermercado / Refeições','Domicílio','Variável')
,('Combustível','Domicílio','Variável')
,('Multa','Domicílio','Variável')
,('Estacionamento','Domicílio','Variável')
,('Simples nacional','Clínica','Variável')
,('Comissões','Geral','Variável');

-- Sanity checks
SELECT * FROM SERVICES;
SELECT * FROM COMMISSION;
SELECT * FROM DEBTS_TYPES;
