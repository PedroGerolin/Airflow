-- Ajuste de 21/09/2026 (roda depois de 01..03; idempotente).
-- 1) NomeContato = "COMO CHAMAR" a pessoa na mensagem: so o PRIMEIRO NOME (o nome completo do cliente ficava
--    longo demais: "Bom dia, Maria Aparecida da Silva!"). Mexe so em contatos que o usuario ainda nao tocou
--    (nao conferidos e com a observacao de seed). Nomes compostos (Ana Paula) o usuario corrige na tela.
-- 2) Rascunhos das mensagens: quem faz a sessao e o ANIMAL, nao o tutor -> marcador {animais}.
-- 3) Descricoes das colunas (documentacao dentro do proprio BigQuery).
UPDATE `gerolingcp.FisioVet_App.contatos`
SET NomeContato = INITCAP(SPLIT(TRIM(NomeContato), ' ')[SAFE_OFFSET(0)]),
    AtualizadoEm = CURRENT_TIMESTAMP()
WHERE RevisadoEm IS NULL
  AND Observacao = 'Semeado do cadastro do Simples Vet'
  AND ARRAY_LENGTH(SPLIT(TRIM(NomeContato), ' ')) > 1;

UPDATE `gerolingcp.FisioVet_App.mensagens`
SET Texto = REPLACE(Texto, 'sessões de {nome_cliente}', 'sessões de {animais}'),
    AtualizadoEm = CURRENT_TIMESTAMP()
WHERE Texto LIKE '%sessões de {nome_cliente}%';

ALTER TABLE `gerolingcp.FisioVet_App.contatos`
  ALTER COLUMN NomeContato SET OPTIONS (description='Como chamar a pessoa na mensagem (ex.: so o primeiro nome). Vira {nome_contato}');

ALTER TABLE `gerolingcp.FisioVet_App.mensagens`
  ALTER COLUMN Texto SET OPTIONS (description='Marcadores: {nome_contato} {nome_cliente} {animais} {mes} {lista_sessoes} {total} {pix}');
