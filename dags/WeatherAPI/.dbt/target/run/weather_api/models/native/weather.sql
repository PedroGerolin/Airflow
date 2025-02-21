
  
    

    create or replace table `gerolingcp`.`WeatherAPI`.`weather`
      
    partition by date
    cluster by location

    OPTIONS()
    as (
      
    WITH dados_brutos AS (
        SELECT 
            semana AS date, 
            name AS location, 
            CAST(tempmax AS FLOAT64) AS temperature_max, 
            CAST(tempmin AS FLOAT64) AS temperature_min 
        FROM `gerolingcp`.`WeatherAPI_External`.`weather_data`
    )

    SELECT *
    FROM dados_brutos
    );
  