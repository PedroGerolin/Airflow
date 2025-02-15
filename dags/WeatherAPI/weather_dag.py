from airflow import DAG # type: ignore
import pendulum
from operators.weather_operator import WeatherOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator 

with DAG(
        dag_id="WeatherAPI",
        start_date=pendulum.datetime(2025, 2, 1, tz="UTC"),
        schedule_interval='@daily', #'0 0 * * 1',  # executar toda segunda feira,
        catchup=True,
) as dag:

        cities = ['SaoPaulo','Calgary']
        start_dt = '{{ ds }}'
        days = 3
        DBT_PROJECT_DIR = '/opt/airflow/dags/WeatherAPI/.dbt'

        weather_operator = WeatherOperator(task_id='weather_operator',
                                        cities=cities,
                                        start_dt=start_dt,
                                        days=days)

start_task = EmptyOperator(
    task_id = 'start_task',
    dag = dag
)    

#dbt run --profiles-dir 'C:\Airflow\dags\WeatherAPI\.dbt' --project-dir 'C:\Airflow\dags\WeatherAPI\.dbt'
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}',
    dag=dag
)

end_task = EmptyOperator(
    task_id = 'end_task',
    dag = dag
)    

start_task >> weather_operator >> dbt_run >> end_task