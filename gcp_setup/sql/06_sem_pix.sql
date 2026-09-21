-- Decisao de 21/09/2026: o usuario envia a chave PIX direto pelo WhatsApp quando precisa, entao o marcador {pix} nao existe.
-- Tira a linha "Chave PIX (CNPJ): {pix}" dos rascunhos e atualiza a descricao dos marcadores. Idempotente.
-- (A tabela configuracoes fica criada e vazia; nao e usada hoje.)
UPDATE `gerolingcp.FisioVet_App.mensagens`
SET Texto = REPLACE(Texto, '\n\nChave PIX (CNPJ): {pix}', ''),
    AtualizadoEm = CURRENT_TIMESTAMP()
WHERE CONTAINS_SUBSTR(Texto, '{pix}');

ALTER TABLE `gerolingcp.FisioVet_App.mensagens`
  ALTER COLUMN Texto SET OPTIONS (description='Marcadores: {nome_contato} {nome_cliente} {animais} {mes} {lista_sessoes} {total}');
