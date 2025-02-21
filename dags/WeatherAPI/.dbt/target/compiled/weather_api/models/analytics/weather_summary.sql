

SELECT 
    location, 
    date, 
    MAX(temperature_max) AS TEMPMAX, 
    MIN(temperature_min) AS TEMPMIN, 
    AVG(temperature_max) AS AVG_TEMPMAX, 
    AVG(temperature_min) AS AVG_TEMPMIN 
FROM `gerolingcp`.`WeatherAPI`.`weather`
GROUP BY location, date