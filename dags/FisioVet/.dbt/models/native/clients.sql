{{ 
    config(
        materialized='table',
        unique_key=['Codigo']
    ) 
}}
    SELECT DISTINCT
        Cliente_Codigo AS Codigo,
        Cliente_Nome AS Nome,
        REPLACE(REPLACE(Cliente_CPF,'.',''),'-','') AS CPF,
        REPLACE(REPLACE(Cliente_RG,'.',''),'-','') AS RG,
        Cliente_Sexo AS Sexo,
        Cliente_Email AS Email,
        Cliente_Telefones AS Telefone,
        Cliente_Endereco AS Endereco,
        Cliente_Bairro AS Bairro,
        Cliente_Cidade AS Cidade,
        Cliente_UF AS UF,
        Cliente_CEP AS CEP,
        Cliente_Datadeinclusao AS DataInclusao,
        Cliente_Datadaultimaatualizacao AS DataUltimaAtualizacao,
        Cliente_Origem AS Origem,
        Cliente_NPS AS NPS,
        Cliente_RankingABC AS RankingABC,
        REPLACE(Cliente_Valorpagonosultimos30dias,'R$ ','') AS ValorPago30,
        REPLACE(Cliente_Valorpagonosultimos90dias,'R$ ','') AS ValorPago90,
        REPLACE(Cliente_Valorpagonosultimos180dias,'R$ ','') AS ValorPago180,
        REPLACE(Cliente_Valorpagonosultimos365dias,'R$ ','') AS ValorPago365,
        REPLACE(Cliente_Ticketmedio,'R$ ','') AS TicketMedio,
        Cliente_Datadaprimeiracompra AS DataPrimeiraCompra,
        Cliente_Ultimavenda AS UltimaVenda,
        Cliente_UltimoacessoaoSimplesPet AS UltimoAcessoSimplesPet,
        Cliente_Tags AS Tags
    FROM `gerolingcp.FisioVet_External.clients_animals`
