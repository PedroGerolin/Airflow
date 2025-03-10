-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `gerolingcp`.`FisioVet`.`sales` as DBT_INTERNAL_DEST
        using (
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
QUALIFY ROW_NUMBER()OVER(PARTITION BY Venda,Codigo,Animal,Produto_Servico ORDER BY Venda) = 1
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.Venda = DBT_INTERNAL_DEST.Venda
                ) and (
                    DBT_INTERNAL_SOURCE.CodigoCliente = DBT_INTERNAL_DEST.CodigoCliente
                ) and (
                    DBT_INTERNAL_SOURCE.NomeAnimal = DBT_INTERNAL_DEST.NomeAnimal
                ) and (
                    DBT_INTERNAL_SOURCE.ProdutoServico = DBT_INTERNAL_DEST.ProdutoServico
                )

    
    when matched then update set
        `DataHora` = DBT_INTERNAL_SOURCE.`DataHora`,`Venda` = DBT_INTERNAL_SOURCE.`Venda`,`Status` = DBT_INTERNAL_SOURCE.`Status`,`DataBaixa` = DBT_INTERNAL_SOURCE.`DataBaixa`,`FormaPagamento` = DBT_INTERNAL_SOURCE.`FormaPagamento`,`Funcionario` = DBT_INTERNAL_SOURCE.`Funcionario`,`NomeCliente` = DBT_INTERNAL_SOURCE.`NomeCliente`,`CodigoCliente` = DBT_INTERNAL_SOURCE.`CodigoCliente`,`NomeAnimal` = DBT_INTERNAL_SOURCE.`NomeAnimal`,`Especie` = DBT_INTERNAL_SOURCE.`Especie`,`SexoAnimal` = DBT_INTERNAL_SOURCE.`SexoAnimal`,`Raca` = DBT_INTERNAL_SOURCE.`Raca`,`TipoItem` = DBT_INTERNAL_SOURCE.`TipoItem`,`Grupo` = DBT_INTERNAL_SOURCE.`Grupo`,`ProdutoServico` = DBT_INTERNAL_SOURCE.`ProdutoServico`,`ValorUnitario` = DBT_INTERNAL_SOURCE.`ValorUnitario`,`Quantidade` = DBT_INTERNAL_SOURCE.`Quantidade`,`Bruto` = DBT_INTERNAL_SOURCE.`Bruto`,`Desconto` = DBT_INTERNAL_SOURCE.`Desconto`,`Liquido` = DBT_INTERNAL_SOURCE.`Liquido`,`Observacoes` = DBT_INTERNAL_SOURCE.`Observacoes`,`date` = DBT_INTERNAL_SOURCE.`date`
    

    when not matched then insert
        (`DataHora`, `Venda`, `Status`, `DataBaixa`, `FormaPagamento`, `Funcionario`, `NomeCliente`, `CodigoCliente`, `NomeAnimal`, `Especie`, `SexoAnimal`, `Raca`, `TipoItem`, `Grupo`, `ProdutoServico`, `ValorUnitario`, `Quantidade`, `Bruto`, `Desconto`, `Liquido`, `Observacoes`, `date`)
    values
        (`DataHora`, `Venda`, `Status`, `DataBaixa`, `FormaPagamento`, `Funcionario`, `NomeCliente`, `CodigoCliente`, `NomeAnimal`, `Especie`, `SexoAnimal`, `Raca`, `TipoItem`, `Grupo`, `ProdutoServico`, `ValorUnitario`, `Quantidade`, `Bruto`, `Desconto`, `Liquido`, `Observacoes`, `date`)


    