select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select temperature_max
from `gerolingcp`.`WeatherAPI`.`weather`
where temperature_max is null



      
    ) dbt_internal_test