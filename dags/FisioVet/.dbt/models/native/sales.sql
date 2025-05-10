{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        transient= false,
        unique_key=['Venda','CodigoCliente','NomeAnimal','ProdutoServico'],
        partition_by={
            "field":"date",
            "data_type": "DATE"
        } 
    ) 
}}
SELECT  
        {% if target.name == 'dev_snowflake' %}
            TO_DATE(Dataehora, 'DD/MM/YYYY HH24:MI') AS DataHora,
        {% else %}
            CAST(Dataehora AS DATETIME FORMAT 'DD/MM/YYYY HH24:MI') AS DataHora,
        {% endif %}
        Venda,
        Statusdavenda AS Status,
        {% if target.name == 'dev_snowflake' %}
            TRY_TO_DATE(Databaixa, 'DD/MM/YYYY') AS DataBaixa,
        {% else %}
            SAFE_CAST(Databaixa AS DATE FORMAT 'DD/MM/YYYY') AS DataBaixa,
        {% endif %}
        Formapagamento AS FormaPagamento,
        Funcionario,
        Cliente AS NomeCliente,
        Codigo AS CodigoCliente,
        Animal AS NomeAnimal,
        Especie,
        Sexo_1 AS SexoAnimal,
        Raca,
        TipodoItem AS TipoItem,
        Grupo,
        Produto_servico AS ProdutoServico,
        CAST(REPLACE(REPLACE(ValorUnitario,'.',''),',','.') AS NUMERIC) AS ValorUnitario,
        Quantidade,
        CAST(REPLACE(REPLACE(Bruto,'.',''),',','.') AS NUMERIC) AS Bruto,
        CAST(REPLACE(REPLACE(Desconto,'.',''),',','.') AS NUMERIC) AS Desconto,
        CAST(REPLACE(REPLACE(Liquido,'.',''),',','.') AS NUMERIC) AS Liquido,
        Observacoes,
        date
    FROM {{ source('FisioVet_External','sales')}}

    {% if is_incremental() %}
       where date >= (select {{ dbt.dateadd("day", -120, "max(date)") }} from {{ this }}) 
    {% endif %}

QUALIFY ROW_NUMBER()OVER(PARTITION BY Venda,Codigo,Animal,Produto_Servico ORDER BY Venda) = 1