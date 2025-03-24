
   
      -- generated script to merge partitions into `gerolingcp`.`FisioVet`.`sales`
      declare dbt_partitions_for_replacement array<date>;

      
      
       -- 1. create a temp table with model data
        
  
    

    create or replace table `gerolingcp`.`FisioVet`.`sales__dbt_tmp`
      
    partition by date
    

    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
    as (
      
SELECT  
        CAST(Dataehora AS DATETIME FORMAT 'DD/MM/YYYY HH24:MI') AS DataHora,
        Venda,
        Statusdavenda AS Status,
        SAFE_CAST(Databaixa AS DATE FORMAT 'DD/MM/YYYY') AS DataBaixa,
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
    FROM `gerolingcp.FisioVet_External.sales`

    
       where date >= (select 

        datetime_add(
            cast( max(date) as datetime),
        interval -90 day
        )

 from `gerolingcp`.`FisioVet`.`sales`) 
    

QUALIFY ROW_NUMBER()OVER(PARTITION BY Venda,Codigo,Animal,Produto_Servico ORDER BY Venda) = 1
    );
  
      -- 2. define partitions to update
      set (dbt_partitions_for_replacement) = (
          select as struct
              -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null
              array_agg(distinct date(date) IGNORE NULLS)
          from `gerolingcp`.`FisioVet`.`sales__dbt_tmp`
      );

      -- 3. run the merge statement
      

    merge into `gerolingcp`.`FisioVet`.`sales` as DBT_INTERNAL_DEST
        using (
        select
        * from `gerolingcp`.`FisioVet`.`sales__dbt_tmp`
      ) as DBT_INTERNAL_SOURCE
        on FALSE

    when not matched by source
         and date(DBT_INTERNAL_DEST.date) in unnest(dbt_partitions_for_replacement) 
        then delete

    when not matched then insert
        (`DataHora`, `Venda`, `Status`, `DataBaixa`, `FormaPagamento`, `Funcionario`, `NomeCliente`, `CodigoCliente`, `NomeAnimal`, `Especie`, `SexoAnimal`, `Raca`, `TipoItem`, `Grupo`, `ProdutoServico`, `ValorUnitario`, `Quantidade`, `Bruto`, `Desconto`, `Liquido`, `Observacoes`, `date`)
    values
        (`DataHora`, `Venda`, `Status`, `DataBaixa`, `FormaPagamento`, `Funcionario`, `NomeCliente`, `CodigoCliente`, `NomeAnimal`, `Especie`, `SexoAnimal`, `Raca`, `TipoItem`, `Grupo`, `ProdutoServico`, `ValorUnitario`, `Quantidade`, `Bruto`, `Desconto`, `Liquido`, `Observacoes`, `date`)

;

      -- 4. clean up the temp table
      drop table if exists `gerolingcp`.`FisioVet`.`sales__dbt_tmp`

  


  

    