from airflow import DAG 
import pendulum
from modules.file_transformer import FileTransformer
from modules.transfer import TransferFile 
from modules.exporter import Exporter 
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group
from airflow.utils.edgemodifier import Label

with DAG(
        dag_id="fisiovet",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        #schedule_interval='@daily', #'0 0 * * 1',  # executar toda segunda feira,
        #schedule_interval='0 0 * * 1',  # executar toda segunda feira,
        catchup=False,
) as dag:
    
    DBT_PROJECT_DIR = '/opt/airflow/dags/FisioVet/.dbt'

    start_task = EmptyOperator(
            task_id = 'start_task',
            dag = dag
        )  
    
    #@task_group(group_id="leitura_arquivos")
    #def leitura_arquivos():
        
    @task()
    def clients_animals_file_transformation():
        file = FileTransformer(
            file_path='/opt/airflow/files/FisioVet/',
            file_name='Animais_e_Clientes.csv',
            new_file_name=f'clients_animals_full.csv')
        file.header_normalize(delete_original_file=True)
    
    @task()
    def sales_file_transformation():
        file = FileTransformer(
            file_path='/opt/airflow/files/FisioVet/',
            file_name='Vendas.csv',
            new_file_name=f'sales.csv')
        file.header_normalize(delete_original_file=True)
        file.split_file('sales.csv','Dataehora','%d/%m/%Y %H:%M',delete_original_file=True)

    @task()
    def debts_file_transformation():
        file = FileTransformer(
            file_path='/opt/airflow/files/FisioVet/',
            file_name='contas-a-pagar.csv',
            new_file_name=f'debts.csv')
        file.header_normalize(delete_original_file=True)
        file.split_file('debts.csv','Data','%d/%m/%Y',delete_original_file=True)

    @task()
    def clients_animals_transfer():
        transfer = TransferFile(
            PathOrig='/opt/airflow/files/FisioVet/',
            PathDest='FisioVet/clients_animals/',
            FileSuffix='clients_animals_full.csv')
        transfer.transfer_file_gcs(delete_original_file=True)
    
    @task()
    def sales_transfer():
        transfer = TransferFile(
            PathOrig='/opt/airflow/files/FisioVet/',
            PathDest='FisioVet/sales/date=',
            FilePrefix='sales_',
            DynamicPath=True)
        transfer.transfer_file_gcs(delete_original_file=True)
    
    @task()
    def debts_transfer():
        transfer = TransferFile(
            PathOrig='/opt/airflow/files/FisioVet/',
            PathDest='FisioVet/debts/date=',
            FilePrefix='debts_',
            DynamicPath=True)
        transfer.transfer_file_gcs(delete_original_file=True)
    
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
Label("Buscar e Normalização dos arquivos") >> [clients_animals_file_transformation(), sales_file_transformation(), debts_file_transformation()] >> \
Label("Envio para GCS") >> [clients_animals_transfer(), sales_transfer(), debts_transfer()] >> \
Label("Executa o DBT") >> dbt_run >> \
Label("Finaliza o processo") >> end_task    