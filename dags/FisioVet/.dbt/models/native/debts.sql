{{ 
    config(
        materialized='incremental',
        unique_key=['Data', 'Descricao', 'Fornecedor'],
        partition_by={
            "field":"date",
            "data_type": "DATE"
        } 
    ) 
}}
    SELECT 
        Data,
        Conta,
        Categoria,
        Descricao,
        Fornecedor,
        Parcela,
        Competencia,
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
