{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
        transient= false,
        unique_key=['Venda','CodigoCliente','NomeAnimal','ProdutoServico'] if target.type == 'bigquery' else 'date',
        partition_by={
            "field":"date",
            "data_type": "DATE"
        } 
    )
}}
{# insert_overwrite: no BigQuery troca só as partições do lote; no Snowflake vira INSERT OVERWRITE e ESVAZIA a
   tabela inteira (ficava só a janela de 120 dias). Lá usamos delete+insert por `date`, equivalente por partição. #}
{# No Snowflake, NUMERIC sem parâmetros vira NUMBER(38,0) e arredonda os centavos; no BigQuery NUMERIC já guarda 9 casas #}
{% set money = 'NUMERIC(18,2)' if target.type == 'snowflake' else 'NUMERIC' %}
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
        CAST(REPLACE(REPLACE(ValorUnitario,'.',''),',','.') AS {{ money }}) AS ValorUnitario,
        Quantidade,
        CAST(REPLACE(REPLACE(Bruto,'.',''),',','.') AS {{ money }}) AS Bruto,
        CAST(REPLACE(REPLACE(Desconto,'.',''),',','.') AS {{ money }}) AS Desconto,
        CAST(REPLACE(REPLACE(Liquido,'.',''),',','.') AS {{ money }}) AS Liquido,
        Observacoes,
        date
    FROM {{ source('FisioVet_External','sales')}}

    {% if is_incremental() %}
       where date >= (select {{ dbt.dateadd("day", -120, "max(date)") }} from {{ this }}) 
    {% endif %}

QUALIFY ROW_NUMBER()OVER(PARTITION BY Venda,Codigo,Animal,Produto_Servico ORDER BY Venda) = 1