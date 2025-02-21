
    WITH dados_brutos AS (
        SELECT semana as date, name as location, tempmax as temperature_max, tempmin as temperature_min 
        FROM `gerolingcp.WeatherAPI_External.weather_data` 
    )

    SELECT *
    FROM dados_brutos