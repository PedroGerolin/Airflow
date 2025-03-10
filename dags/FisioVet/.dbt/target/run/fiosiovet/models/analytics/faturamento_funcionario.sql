
  
    

    create or replace table `gerolingcp`.`FisioVet_Analytics`.`faturamento_funcionario`
      
    
    

    OPTIONS()
    as (
      
    SELECT 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora) AS AnoMes
        ,COUNT(Venda) AS Atendimentos
        ,SUM(Liquido) AS ValorLiquido
    FROM `gerolingcp`.`FisioVet`.`sales` 
    GROUP BY 
        Funcionario
        ,FORMAT_DATE('%Y-%m-01', DataHora)
    );
  