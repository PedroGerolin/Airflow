{{
    config(
        materialized='view',
        schema='Analytics',
        enabled=(target.type == 'bigquery')
    )
}}
{# Sessoes AINDA DEVIDAS ate o corte do ciclo atual. So BigQuery: as tabelas FisioVet_App nao existem no Snowflake.
   Corte: ciclo mais recente iniciado no app (ex.: 2026-08-01) => entram TODAS as sessoes em aberto com data
   anterior a 01/09, ou seja, Agosto e tudo antes; nada do mes corrente. Sem ciclo iniciado => view vazia.
   'Baixa parcial' conta como devendo (decisao de 20/09/2026). #}
WITH ciclo AS (
    SELECT MAX(MesReferencia) AS MesCiclo
    FROM {{ source('FisioVet_App', 'ciclos') }}
)
SELECT
    S.CodigoCliente,
    S.NomeCliente,
    S.Venda,
    DATE(S.DataHora) AS DataSessao,
    DATE_TRUNC(DATE(S.DataHora), MONTH) AS MesSessao,
    S.NomeAnimal,
    S.ProdutoServico,
    S.Status,
    S.Status = 'Baixa parcial' AS Parcial,
    S.Liquido AS Valor,
    C.MesCiclo
FROM {{ ref('sales') }} S
CROSS JOIN ciclo C
WHERE S.Status IN ('Aberto', 'Baixa parcial')
  AND DATE(S.DataHora) < DATE_ADD(C.MesCiclo, INTERVAL 1 MONTH)
