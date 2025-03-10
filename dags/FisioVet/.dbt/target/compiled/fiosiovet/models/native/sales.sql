
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