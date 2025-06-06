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
    FROM {{ source('FisioVet_External','debts')}} 
