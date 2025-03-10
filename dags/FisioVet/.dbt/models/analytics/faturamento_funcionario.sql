{{ 
    config(
        materialized='incremental',
        unique_key=['Funcionario','AnoMes'],
        schema='Analytics' 
    ) 
}}
    SELECT 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
    FROM {{ ref('sales') }} 
    GROUP BY 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora)
