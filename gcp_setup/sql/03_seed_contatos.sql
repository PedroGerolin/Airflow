-- Cadastro INICIAL de contatos de cobranca, gerado a partir do cadastro do Simples Vet (clients.Telefone).
-- Semeia so quem tem algo em aberto (Aberto ou Baixa parcial). Os dados NAO passam por arquivo nem pelo repo:
-- o INSERT ... SELECT roda dentro do BigQuery.
-- - Telefone: primeiro celular (9 digitos, comeca com 9) do texto livre "(11) 99999-9999, (11) 3333-3333";
--   vira digitos com DDI (55...). Sem celular valido => NULL (o app avisa "sem telefone valido").
-- - NomeContato = "como chamar" na mensagem: primeiro nome do cliente (o usuario troca na tela quando for outra pessoa).
-- - RevisadoEm NULL: o usuario ainda nao conferiu.
-- Idempotente: nao mexe em quem ja tem linha.
INSERT INTO `gerolingcp.FisioVet_App.contatos`
  (CodigoCliente, NomeContato, TelefoneWhatsapp, Situacao, NaoCobrarNoCiclo, Observacao, RevisadoEm, AtualizadoEm)
WITH devedores AS (
  SELECT DISTINCT CAST(CodigoCliente AS INT64) AS CodigoCliente
  FROM `gerolingcp.FisioVet.sales`
  WHERE Status IN ('Aberto', 'Baixa parcial')
),
cadastro AS (
  SELECT CAST(Codigo AS INT64) AS Codigo, Nome, Telefone
  FROM `gerolingcp.FisioVet.clients`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Codigo ORDER BY Nome) = 1
)
SELECT
  d.CodigoCliente,
  INITCAP(SPLIT(TRIM(c.Nome), ' ')[SAFE_OFFSET(0)]),
  CONCAT('55', REGEXP_REPLACE(REGEXP_EXTRACT(c.Telefone, r'(\(\d{2}\)\s*9\d{4}-?\d{4})'), r'\D', '')),
  'ATIVO',
  CAST(NULL AS DATE),
  'Semeado do cadastro do Simples Vet',
  CAST(NULL AS TIMESTAMP),
  CURRENT_TIMESTAMP()
FROM devedores d
LEFT JOIN cadastro c ON c.Codigo = d.CodigoCliente
WHERE NOT EXISTS (
  SELECT 1 FROM `gerolingcp.FisioVet_App.contatos` x WHERE x.CodigoCliente = d.CodigoCliente
);
