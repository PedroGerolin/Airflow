select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select location
from `gerolingcp`.`WeatherAPI`.`weather`
where location is null



      
    ) dbt_internal_test