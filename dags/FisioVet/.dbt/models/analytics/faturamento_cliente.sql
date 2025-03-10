{{ 
    config(
        materialized='incremental',
        unique_key=['CodigoCliente','Nome','ProdutoServico','AnoMes'],
        schema='Analytics' 
    ) 
}}
    SELECT 
        S.CodigoCliente
        ,C.Nome
        ,S.ProdutoServico
        ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
        ,SUM(IF(Status = 'Baixado',Liquido,0)) AS ValorRecebido
        ,SUM(IF(Status = 'Aberto',Liquido,0)) AS ValorEmAberto
    FROM {{ ref('sales') }} S
    JOIN {{ ref('clients') }} C 
        ON S.CodigoCliente = C.Codigo
    WHERE 
        date > "2023-01-01" 
    GROUP BY 
        S.CodigoCliente
        ,C.Nome
        ,S.ProdutoServico
        ,FORMAT_DATE('%Y-%m-01', DataHora)
