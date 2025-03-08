{{ 
    config(
        materialized='table',
        unique_key=['Codigo']
    ) 
}}
    SELECT 
        Animal_Codigo AS Codigo,
        Cliente_Codigo,
        Animal_Nome AS Nome,
        Animal_Especie AS Especie,
        Animal_Raca AS Raca,
        Animal_Pelagem AS Pelagem,
        Animal_Esterilizacao AS Esterilizacao,
        Animal_Nascimento AS DataNascimento,
        Animal_Sexo AS Sexo,
        Animal_Pedigree AS Pedigree,
        Animal_Chip AS Chip,
        Animal_Tags AS Tags,
        Animal_Status AS Status,
        Animal_Datadeinclusao AS DataInclusao,
        Animal_Vivo_Morto AS VivoMorto
    FROM `gerolingcp.FisioVet_External.clients_animals`
