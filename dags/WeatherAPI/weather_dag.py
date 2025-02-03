from airflow import DAG
import pendulum
from operators.weather_operator import WeatherOperator

with DAG(
        "WeatherAPI",
        start_date=pendulum.datetime(2025, 2, 1, tz="UTC"),
        schedule_interval='@daily' #'0 0 * * 1',  # executar toda segunda feira
) as dag:

        cities = ['SaoPaulo','Calgary']
        start_dt = '{{ ds }}'
        days = 3

        weather_operator = WeatherOperator(task_id='weather_operator',
                                        cities=cities,
                                        start_dt=start_dt,
                                        days=days)
