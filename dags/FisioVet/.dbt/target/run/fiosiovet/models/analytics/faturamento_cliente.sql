-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `gerolingcp`.`FisioVet_Analytics`.`faturamento_cliente` as DBT_INTERNAL_DEST
        using (
    SELECT 
        S.CodigoCliente
        ,C.Nome
        ,S.ProdutoServico
        ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
        ,SUM(IF(Status = 'Baixado',Liquido,0)) AS ValorRecebido
        ,SUM(IF(Status = 'Aberto',Liquido,0)) AS ValorEmAberto
    FROM `gerolingcp`.`FisioVet`.`sales` S
    JOIN `gerolingcp`.`FisioVet`.`clients` C 
        ON S.CodigoCliente = C.Codigo
    WHERE 
        date > "2023-01-01" 
    GROUP BY 
        S.CodigoCliente
        ,C.Nome
        ,S.ProdutoServico
        ,FORMAT_DATE('%Y-%m-01', DataHora)
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.CodigoCliente = DBT_INTERNAL_DEST.CodigoCliente
                ) and (
                    DBT_INTERNAL_SOURCE.Nome = DBT_INTERNAL_DEST.Nome
                ) and (
                    DBT_INTERNAL_SOURCE.ProdutoServico = DBT_INTERNAL_DEST.ProdutoServico
                ) and (
                    DBT_INTERNAL_SOURCE.AnoMes = DBT_INTERNAL_DEST.AnoMes
                )

    
    when matched then update set
        `CodigoCliente` = DBT_INTERNAL_SOURCE.`CodigoCliente`,`Nome` = DBT_INTERNAL_SOURCE.`Nome`,`ProdutoServico` = DBT_INTERNAL_SOURCE.`ProdutoServico`,`AnoMes` = DBT_INTERNAL_SOURCE.`AnoMes`,`Atendimentos` = DBT_INTERNAL_SOURCE.`Atendimentos`,`ValorLiquido` = DBT_INTERNAL_SOURCE.`ValorLiquido`,`ValorRecebido` = DBT_INTERNAL_SOURCE.`ValorRecebido`,`ValorEmAberto` = DBT_INTERNAL_SOURCE.`ValorEmAberto`
    

    when not matched then insert
        (`CodigoCliente`, `Nome`, `ProdutoServico`, `AnoMes`, `Atendimentos`, `ValorLiquido`, `ValorRecebido`, `ValorEmAberto`)
    values
        (`CodigoCliente`, `Nome`, `ProdutoServico`, `AnoMes`, `Atendimentos`, `ValorLiquido`, `ValorRecebido`, `ValorEmAberto`)


    