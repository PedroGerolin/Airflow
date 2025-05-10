SELECT
  Venda
FROM
  {{ ref('sales') }}
WHERE Liquido < 0