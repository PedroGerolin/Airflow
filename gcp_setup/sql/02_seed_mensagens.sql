-- Modelos INICIAIS (rascunho) — o usuario edita depois na tela do app. Sem dado pessoal.
-- Idempotente: so insere o que ainda nao existe.
INSERT INTO `gerolingcp.FisioVet_App.mensagens` (Nome, Texto, EhInicial, Ativo, AtualizadoEm)
SELECT Nome, Texto, EhInicial, TRUE, CURRENT_TIMESTAMP()
FROM UNNEST([
  STRUCT(
    'Inicial' AS Nome,
    'Bom dia, {nome_contato}! Segue o resumo das sessões de {animais} até {mes}:\n\n{lista_sessoes}\n\n*Total: {total}*\n\nChave PIX (CNPJ): {pix}\n\nQualquer dúvida, é só me chamar!' AS Texto,
    TRUE AS EhInicial),
  STRUCT(
    'Lembrete',
    'Olá, {nome_contato}, tudo bem? Passando para lembrar do pagamento das sessões de {animais} até {mes}, no total de {total}.\n\n{lista_sessoes}\n\nChave PIX (CNPJ): {pix}\n\nSe já realizou o pagamento, por favor desconsidere e me envie o comprovante. Obrigada!',
    FALSE)
]) AS m
WHERE NOT EXISTS (SELECT 1 FROM `gerolingcp.FisioVet_App.mensagens` x WHERE x.Nome = m.Nome);
