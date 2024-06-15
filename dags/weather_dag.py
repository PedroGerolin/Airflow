from airflow import DAG
import pendulum
from operators.weather_operator import WeatherOperator

with DAG(
        "WeatherAPI",
        start_date=pendulum.datetime(2024, 6, 12, tz="UTC"),
        schedule_interval='@daily' #'0 0 * * 1',  # executar toda segunda feira
) as dag:

    city = 'SaoPaulo'
    key = 'QNSRNJVD9U43K2Y4WXYY8F6LK'
    start_dt = '{{ ds }}'
    days = 6

    weather_operator = WeatherOperator(task_id='weather_operator',
                                       city=city,
                                       start_dt=start_dt,
                                       days=days,
                                       key=key,
                                       file_path=f'/opt/airflow/files/WeatherAPI/'
                                                 f'{city}/'
                                                 f'semana={start_dt}'
                                                 f'/dados_')
