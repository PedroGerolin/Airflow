{{ 
    config(
        materialized='incremental',
        unique_key=['Funcionario','AnoMes'],
        schema='Analytics' 
    ) 
}}
    SELECT 
        Funcionario
        {% if target.name == 'prod_bigquery' %}
            ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        {% else %}
            ,TO_CHAR(DataHora, 'YYYY-MM-01') AS AnoMes
        {% endif %}  
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
    FROM {{ ref('sales') }} 
    GROUP BY 
        Funcionario
        {% if target.name == 'prod_bigquery' %}
            ,FORMAT_DATE('%Y-%m-01', DataHora)
        {% else %}
            ,TO_CHAR(DataHora, 'YYYY-MM-01')
        {% endif %}    
        
        
        
