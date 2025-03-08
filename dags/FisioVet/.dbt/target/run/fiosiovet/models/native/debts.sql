-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `gerolingcp`.`FisioVet`.`debts` as DBT_INTERNAL_DEST
        using (
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
        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.Data = DBT_INTERNAL_DEST.Data
                ) and (
                    DBT_INTERNAL_SOURCE.Descricao = DBT_INTERNAL_DEST.Descricao
                ) and (
                    DBT_INTERNAL_SOURCE.Fornecedor = DBT_INTERNAL_DEST.Fornecedor
                )

    
    when matched then update set
        `Data` = DBT_INTERNAL_SOURCE.`Data`,`Conta` = DBT_INTERNAL_SOURCE.`Conta`,`Categoria` = DBT_INTERNAL_SOURCE.`Categoria`,`Descricao` = DBT_INTERNAL_SOURCE.`Descricao`,`Fornecedor` = DBT_INTERNAL_SOURCE.`Fornecedor`,`Parcela` = DBT_INTERNAL_SOURCE.`Parcela`,`Competencia` = DBT_INTERNAL_SOURCE.`Competencia`,`Valor` = DBT_INTERNAL_SOURCE.`Valor`,`Desconto` = DBT_INTERNAL_SOURCE.`Desconto`,`Multa` = DBT_INTERNAL_SOURCE.`Multa`,`Juros` = DBT_INTERNAL_SOURCE.`Juros`,`Vencimento` = DBT_INTERNAL_SOURCE.`Vencimento`,`Pagamento` = DBT_INTERNAL_SOURCE.`Pagamento`,`ValorPago` = DBT_INTERNAL_SOURCE.`ValorPago`,`FormaPagamento` = DBT_INTERNAL_SOURCE.`FormaPagamento`,`DocumentoNF` = DBT_INTERNAL_SOURCE.`DocumentoNF`,`Observacao` = DBT_INTERNAL_SOURCE.`Observacao`,`date` = DBT_INTERNAL_SOURCE.`date`
    

    when not matched then insert
        (`Data`, `Conta`, `Categoria`, `Descricao`, `Fornecedor`, `Parcela`, `Competencia`, `Valor`, `Desconto`, `Multa`, `Juros`, `Vencimento`, `Pagamento`, `ValorPago`, `FormaPagamento`, `DocumentoNF`, `Observacao`, `date`)
    values
        (`Data`, `Conta`, `Categoria`, `Descricao`, `Fornecedor`, `Parcela`, `Competencia`, `Valor`, `Desconto`, `Multa`, `Juros`, `Vencimento`, `Pagamento`, `ValorPago`, `FormaPagamento`, `DocumentoNF`, `Observacao`, `date`)


    