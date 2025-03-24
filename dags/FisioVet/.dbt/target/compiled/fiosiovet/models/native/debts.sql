
    SELECT 
        Data,
        Conta,
        Categoria,
        Descricao,
        Fornecedor,
        Parcela,
        Data as Competencia,
        Valor,
        Desconto,
        Multa,
        Juros,
        Vencimento,
        Pagamento,
        Valorpago AS ValorPago,
        Formadepagamento AS FormaPagamento,
        Documento_NF AS DocumentoNF,
        Observacao,
        date
    FROM `gerolingcp.FisioVet_External.debts`