-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `gerolingcp`.`FisioVet_Analytics`.`faturamento_funcionario` as DBT_INTERNAL_DEST
        using (
    SELECT 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
    FROM `gerolingcp`.`FisioVet`.`sales` 
    GROUP BY 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora)
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.Funcionario = DBT_INTERNAL_DEST.Funcionario
                ) and (
                    DBT_INTERNAL_SOURCE.AnoMes = DBT_INTERNAL_DEST.AnoMes
                )

    
    when matched then update set
        `Funcionario` = DBT_INTERNAL_SOURCE.`Funcionario`,`AnoMes` = DBT_INTERNAL_SOURCE.`AnoMes`,`Atendimentos` = DBT_INTERNAL_SOURCE.`Atendimentos`,`ValorLiquido` = DBT_INTERNAL_SOURCE.`ValorLiquido`
    

    when not matched then insert
        (`Funcionario`, `AnoMes`, `Atendimentos`, `ValorLiquido`)
    values
        (`Funcionario`, `AnoMes`, `Atendimentos`, `ValorLiquido`)


    