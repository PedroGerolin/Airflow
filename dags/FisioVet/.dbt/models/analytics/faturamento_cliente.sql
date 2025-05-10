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
        {% if target.name == 'prod_bigquery' %}
            ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        {% else %}
            ,TO_CHAR(DataHora, 'YYYY-MM-01') AS AnoMes
        {% endif %}        
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
        {% if target.name == 'prod_bigquery' %}
            ,SUM(IF(Status = 'Baixado',Liquido,0)) AS ValorRecebido
            ,SUM(IF(Status = 'Aberto',Liquido,0)) AS ValorEmAberto
        {% else %}
            ,SUM(IFF(Status = 'Baixado',Liquido,0)) AS ValorRecebido
            ,SUM(IFF(Status = 'Aberto',Liquido,0)) AS ValorEmAberto
        {% endif %} 
    FROM {{ ref('sales') }} S
    JOIN {{ ref('clients') }} C 
        ON S.CodigoCliente = C.Codigo 
    WHERE 
        date > '2023-01-01'
    GROUP BY 
        S.CodigoCliente
        ,C.Nome
        ,S.ProdutoServico
        {% if target.name == 'prod_bigquery' %}
            ,FORMAT_DATE('%Y-%m-01', DataHora)
        {% else %}
            ,TO_CHAR(DataHora, 'YYYY-MM-01')
        {% endif %}    
        
