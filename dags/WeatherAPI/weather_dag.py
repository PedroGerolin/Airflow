from airflow import DAG 
import pendulum
from operators.weather_operator import WeatherOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from modules.converter import Converter
from modules.transfer import TransferFile 
from airflow.decorators import task
from airflow.utils.edgemodifier import Label

with DAG(
        dag_id="WeatherAPI",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        #schedule_interval='@daily', #'0 0 * * 1',  # executar toda segunda feira,
        schedule_interval='0 0 * * 1',  # executar toda segunda feira,
        catchup=True,
) as dag:

        cities = ['SaoPaulo','Calgary','Assis']
        start_dt = '{{ ds }}'
        days = 6
        DBT_PROJECT_DIR = '/opt/airflow/dags/WeatherAPI/.dbt'

        start_task = EmptyOperator(
            task_id = 'start_task',
            dag = dag
        )  
        
        weather_operator = WeatherOperator(
            task_id='weather_operator',
            cities=cities,
            start_dt=start_dt,
            days=days
        )

        @task()
        def conversao(start_dt):
            for city in cities:
                converter = Converter(
                    'csv',
                    'orc',
                    f'/opt/airflow/files/WeatherAPI/{city}/semana={start_dt}/',
                    'dados_brutos.csv')
                converter.convert()

        @task()
        def transfer(start_dt):
            for city in cities:
                PathOrig=f'/opt/airflow/files/WeatherAPI/{city}/semana={start_dt}/'
                PathDest=f'WeatherAPI/city={city}/semana={start_dt}/'
                transfer = TransferFile(
                    PathOrig=PathOrig,
                    PathDest=PathDest,
                    FileSuffix='dados_brutos.orc'
                )
                transfer.transfer_file_gcs()
        
        dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command=f'dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}',
            dag=dag
        )
        dbt_run = BashOperator(
            task_id='dbt_run',
            bash_command=f'dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}',
            dag=dag
        )

        end_task = EmptyOperator(
            task_id = 'end_task',
            dag = dag
        )    

start_task >> \
Label("Buscar dados por API") >> weather_operator >> \
Label("Faz a conversão de arquivos") >> conversao(start_dt) >> \
Label("Faz o envio de arquivos") >> transfer(start_dt) >> \
Label("Executa os testes do DBT") >> dbt_test >> \
Label("Executa o DBT") >> dbt_run >> \
Label("Finaliza o processo") >> end_task