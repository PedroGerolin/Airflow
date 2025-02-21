{{ 
    config(
        materialized='table',
        cluster_by='location',
        partition_by={
            "field":"date",
            "data_type": "DATE"
        },
        schema='Analytics'  
    ) 
}}

SELECT 
    location, 
    date, 
    MAX(temperature_max) AS TEMPMAX, 
    MIN(temperature_min) AS TEMPMIN, 
    AVG(temperature_max) AS AVG_TEMPMAX, 
    AVG(temperature_min) AS AVG_TEMPMIN 
FROM {{ ref('weather') }}
GROUP BY location, date