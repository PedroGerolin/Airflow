
  
    

    create or replace table `gerolingcp`.`FisioVet_Analytics`.`resultado_operacional`
      
    
    

    OPTIONS()
    as (
      
WITH Receita AS 
(
  SELECT
    FORMAT_DATE('%Y-%m-01', date) AS Mes,
    SE.Local,
    ROUND(SUM(CAST(Liquido AS FLOAT64)),2) AS ValorTotal,
    COUNT(1) AS Atendimentos
  FROM `gerolingcp.FisioVet.sales` SA
  JOIN `gerolingcp.FisioVet.services` SE
    ON SA.ProdutoServico = SE.Servico
  GROUP BY
    FORMAT_DATE('%Y-%m-01', date),
    SE.Local
),
CustoFixo AS
(
  SELECT 
    FORMAT_DATE('%Y-%m-01', date) AS Mes,
    DT.Local,
    ROUND(SUM(CAST(ValorPago AS FLOAT64)),2) AS TotalPago
  FROM `gerolingcp`.`FisioVet`.`debts`  D
  JOIN `gerolingcp.FisioVet.debts_types` DT
    ON D.Categoria = DT.CategoriaDebito
  WHERE DT.Tipo = 'Fixo'
  GROUP BY 
    FORMAT_DATE('%Y-%m-01', date),
    Local
)
,CustoVariavel AS
(
  SELECT 
    FORMAT_DATE('%Y-%m-01', date) AS Mes,
    DT.Local,
    ROUND(SUM(CAST(D.ValorPago AS FLOAT64)),2) AS TotalPago
  FROM `gerolingcp`.`FisioVet`.`debts`  D
  JOIN `gerolingcp.FisioVet.debts_types` DT
    ON D.Categoria = DT.CategoriaDebito
  WHERE DT.Tipo = 'Variável'
  GROUP BY 
    FORMAT_DATE('%Y-%m-01', date),
    DT.Local
)
,Comissoes AS
(
  SELECT
    FORMAT_DATE('%Y-%m-01', date) AS Mes,
    SE.Local,
    ROUND(SUM((CAST(S.Liquido AS FLOAT64)*(C.Comissao/100))),2) AS Comissao
  FROM `gerolingcp`.`FisioVet`.`sales`  S
  JOIN `FisioVet.commission` C
    ON S.Funcionario = C.Funcionario
    AND S.ProdutoServico = C.ProdutoServico
  JOIN `FisioVet.services` SE
    ON S.ProdutoServico = SE.Servico
  GROUP BY
    FORMAT_DATE('%Y-%m-01', date),
    SE.Local
)
SELECT 
  R.Mes, 
  R.Local,
  R.Atendimentos,
  R.ValorTotal AS Receita,
  ROUND(
    (R.ValorTotal / R.Atendimentos)
  ,2) AS TicketMedio,
  IFNULL(CF.TotalPago,0) AS CustoFixo,
  ROUND(
    (IFNULL(CF.TotalPago,0) / R.Atendimentos)
  ,2) AS CustoFixoPorAtendimento,
  ROUND(
    (R.ValorTotal - IFNULL(CF.TotalPago,0))
  ,2) AS LucroBruto, 
  IFNULL(CV.TotalPago,0) AS CustoVariavel,
  ROUND(
    (IFNULL(CV.TotalPago,0) / R.Atendimentos)
  ,2) AS CustoVariavelPorAtendimento,
  IFNULL(CC.Comissao,0) AS CustoComissoes,
  ROUND(
    (IFNULL(CC.Comissao,0) / R.Atendimentos)
  ,2) AS CustoComissoesPorAtendimento,
  ROUND(
    (IFNULL(CC.Comissao,0) / R.ValorTotal )  * 100
  ,2) AS PorcentagemMediaComissoes,
  ROUND(
    (R.ValorTotal - IFNULL(CF.TotalPago,0) - IFNULL(CV.TotalPago,0) - IFNULL(CC.Comissao,0)
  ),2) AS LucroLiquido,
  ROUND(
    ((R.ValorTotal - IFNULL(CF.TotalPago,0) - IFNULL(CV.TotalPago,0) - IFNULL(CC.Comissao,0)) / R.Atendimentos)
  ,2) AS LucroLiquidoPorAtendimento
FROM Receita R
LEFT JOIN CustoFixo CF
  ON R.Mes = CF.Mes
  AND R.Local = CF.Local
LEFT JOIN CustoVariavel CV
  ON R.Mes = CV.Mes
  AND R.Local = CV.Local
LEFT JOIN Comissoes CC
  ON R.Mes = CC.Mes
  AND R.Local = CC.Local
    );
  