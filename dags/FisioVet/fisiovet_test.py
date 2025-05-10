from airflow import DAG 
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.edgemodifier import Label


default_args = {
    'DBT_PROJECT_DIR': '/opt/airflow/dags/FisioVet/.dbt',
    'fisiovet_file_path': '/opt/airflow/files/FisioVet/'
}
@dag(
        dag_id="fisiovet_test",
        default_args=default_args,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule_interval=None,  # Não executar automaticamente
        #schedule_interval='@daily',  # executar diariamente
        #schedule_interval='0 0 * * 1',  # executar toda segunda feira,
        catchup=False
)
def fisiovet_test_dag():    


    dbt_run = BashOperator(
             task_id='dbt_run',
             bash_command=f'dbt run --profiles-dir {default_args["DBT_PROJECT_DIR"]} --project-dir {default_args["DBT_PROJECT_DIR"]}'
         )
    
 
    
    dbt_run 


fisiovet_test_dag()