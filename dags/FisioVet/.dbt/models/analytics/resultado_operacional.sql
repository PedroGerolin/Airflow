{% if target.name == 'prod_bigquery' %}
  {{ 
      config(
          materialized='table',
          schema='Analytics' 
      ) 
  }}
  WITH Receita AS 
  (
    SELECT
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date) AS Mes,
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01') AS Mes,
      {% endif %} 
      SE.Local,
      SA.Funcionario,
      {% if target.name == 'prod_bigquery' %}
          ROUND(SUM(CAST(Liquido AS FLOAT64)),2) AS ValorTotal,
      {% else %}
          ROUND(SUM(CAST(Liquido AS FLOAT)),2) AS ValorTotal,
      {% endif %} 
      COUNT(1) AS Atendimentos
    FROM {{ ref('sales') }}  SA
    JOIN {{ source('FisioVet','services')}} SE
      ON SA.ProdutoServico = SE.Servico
    GROUP BY
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date),
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01'),
      {% endif %} 
      SE.Local,
      SA.Funcionario
  ),
  ReceitaTotal AS 
  (
    SELECT
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date) AS Mes,
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01') AS Mes,
      {% endif %} 
      SE.Local,
      {% if target.name == 'prod_bigquery' %}
          ROUND(SUM(CAST(Liquido AS FLOAT64)),2) AS ValorTotal,
      {% else %}
          ROUND(SUM(CAST(Liquido AS FLOAT)),2) AS ValorTotal,
      {% endif %} 
      COUNT(1) AS Atendimentos
    FROM {{ ref('sales') }}  SA
    JOIN {{ source('FisioVet','services')}} SE
      ON SA.ProdutoServico = SE.Servico
    GROUP BY
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date),
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01'),
      {% endif %} 
      SE.Local
  ),
  CustoFixo AS
  (
    SELECT 
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date) AS Mes,
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01') AS Mes,
      {% endif %} 
      DT.Local,
      {% if target.name == 'prod_bigquery' %}
          ROUND(SUM(CAST(ValorPago AS FLOAT64)),2) AS TotalPago
      {% else %}
          ROUND(SUM(CAST(ValorPago AS FLOAT)),2) AS TotalPago
      {% endif %} 
    FROM {{ ref('debts') }}  D
    JOIN {{ source('FisioVet','debts_types')}} DT
      ON D.Categoria = DT.CategoriaDebito
    WHERE DT.Tipo = 'Fixo'
    GROUP BY 
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date),
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01'),
      {% endif %} 
      Local
  )
  ,CustoVariavel AS
  (
    SELECT 
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date) AS Mes,
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01') AS Mes,
      {% endif %} 
      DT.Local,
      {% if target.name == 'prod_bigquery' %}
          ROUND(SUM(CAST(D.ValorPago AS FLOAT64)),2) AS TotalPago
      {% else %}
          ROUND(SUM(CAST(D.ValorPago AS FLOAT)),2) AS TotalPago
      {% endif %} 
    FROM {{ ref('debts') }}  D
    JOIN {{ source('FisioVet','debts_types')}} DT
      ON D.Categoria = DT.CategoriaDebito
    WHERE DT.Tipo = 'Variável'
    GROUP BY 
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date),
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01'),
      {% endif %} 
      DT.Local
  )
  ,Comissoes AS
  (
    SELECT
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date) AS Mes,
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01') AS Mes,
      {% endif %} 
      SE.Local,
      S.Funcionario,
      {% if target.name == 'prod_bigquery' %}
          ROUND(SUM((CAST(S.Liquido AS FLOAT64)*(C.Comissao/100))),2) AS Comissao
      {% else %}
          ROUND(SUM((CAST(S.Liquido AS FLOAT)*(C.Comissao/100))),2) AS Comissao
      {% endif %} 
    FROM {{ ref('sales') }}  S
    JOIN {{ source('FisioVet','commission')}} C
      ON S.Funcionario = C.Funcionario
      AND S.ProdutoServico = C.ProdutoServico
    JOIN {{ source('FisioVet','services')}} SE
      ON S.ProdutoServico = SE.Servico
    GROUP BY
      {% if target.name == 'prod_bigquery' %}
          FORMAT_DATE('%Y-%m-01', date),
      {% else %}
          TO_CHAR(date, 'YYYY-MM-01'),
      {% endif %} 
      SE.Local,
      S.Funcionario
  )
  SELECT 
    R.Mes, 
    R.Local,
    R.Funcionario,
    R.Atendimentos,
    R.ValorTotal AS ReceitaTotal,
    ROUND(
      (R.ValorTotal / R.Atendimentos)
    ,2) AS TicketMedio,
    ROUND(
      ((IFNULL(CF.TotalPago,0) / (SELECT Atendimentos FROM ReceitaTotal RE WHERE RE.Mes = R.Mes AND RE.Local = R.Local))* R.Atendimentos)
    ,2) AS CustoFixo,
    ROUND(
      ((IFNULL(CV.TotalPago,0) / (SELECT Atendimentos FROM ReceitaTotal RE WHERE RE.Mes = R.Mes AND RE.Local = R.Local))* R.Atendimentos)
    ,2) AS CustoVariavel,
    IFNULL(CC.Comissao,0) AS CustoComissoes,
    ROUND(
      (IFNULL(CC.Comissao,0) / R.Atendimentos)
    ,2) AS CustoMedioComissoes,
    ROUND(
      (IFNULL(CC.Comissao,0) / R.ValorTotal )  * 100
    ,2) AS PorcentagemMediaComissoes,
    ROUND(
      (((R.ValorTotal - IFNULL(CF.TotalPago,0) - IFNULL(CV.TotalPago,0) - IFNULL(CC.Comissao,0)) / (SELECT Atendimentos FROM ReceitaTotal RE WHERE RE.Mes = R.Mes AND RE.Local = R.Local))* R.Atendimentos)
    ,2) AS LucroLiquido
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
    AND R.Funcionario = CC.Funcionario
{% endif %} 