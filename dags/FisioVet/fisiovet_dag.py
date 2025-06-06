from airflow import DAG 
import pendulum
from modules.file_transformer import FileTransformer
from modules.transfer import TransferFile 
from modules.exporter import Exporter 
from modules.fisiovet_downloader import fisioVetDownloader
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.edgemodifier import Label


default_args = {
    'DBT_PROJECT_DIR': '/opt/airflow/dags/FisioVet/.dbt',
    'fisiovet_file_path': '/opt/airflow/files/FisioVet/'
}
@dag(
        dag_id="fisiovet",
        default_args=default_args,
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule_interval=None,  # Não executar automaticamente
        #schedule_interval='@daily',  # executar diariamente
        #schedule_interval='0 0 * * 1',  # executar toda segunda feira,
        catchup=False,
        tags=['fisiovet','bigquery','GCS','dbt','ELT']
)
def fisiovet_dag():    

    start_task = EmptyOperator(
            task_id = 'start_task'
        )  

    @task_group()
    def fisiovet_downloader():    
        def iniciarfisioVetDownloader():
            fisioVet = fisioVetDownloader()
            fisioVet.iniciar_navegador()
            fisioVet.realizar_login()
            return fisioVet
        
        @task()
        def download_clients_file():
            fisioVetClients = iniciarfisioVetDownloader()
            fisioVetClients.enter_clients_page()
            fisioVetClients.export_clients()
        
        @task()
        def download_sales_file():
            fisioVetSales = iniciarfisioVetDownloader()
            fisioVetSales.enter_sales_page()
            fisioVetSales.export_sales()
    
        download_clients_file() >> download_sales_file()

    @task_group()
    def file_transformation():
        @task()
        def clients_animals_file_transformation():
            file = FileTransformer(
                file_path=default_args['fisiovet_file_path'],
                file_name='Animais_e_Clientes.csv',
                new_file_name=f'clients_animals_full.csv')
            file.header_normalize(delete_original_file=True)
        
        @task()
        def sales_file_transformation():
            file = FileTransformer(
                file_path=default_args['fisiovet_file_path'],
                file_name='Vendas.csv',
                new_file_name=f'sales.csv')
            file.header_normalize(delete_original_file=True)
            file.split_file('sales.csv','Dataehora','%d/%m/%Y %H:%M',delete_original_file=True)

        @task()
        def debts_file_transformation():
            file = FileTransformer(
                file_path=default_args['fisiovet_file_path'],
                file_name='contas-a-pagar.csv',
                new_file_name=f'debts.csv', 
                sep=','
            )
            file.header_normalize(delete_original_file=True)
            file.split_file('debts.csv','Data','%d/%m/%Y',delete_original_file=True)

        clients_animals_file_transformation() 
        sales_file_transformation() 
        debts_file_transformation()

    @task_group()
    def file_transfer():
        @task()
        def clients_animals_transfer():
            transfer = TransferFile(
                PathOrig=default_args['fisiovet_file_path'],
                PathDest='FisioVet/clients_animals/',
                FileSuffix='clients_animals_full.csv')
            transfer.transfer_file_gcs(delete_original_file=True)
        
        @task()
        def sales_transfer():
            transfer = TransferFile(
                PathOrig=default_args['fisiovet_file_path'],
                PathDest='FisioVet/sales/date=',
                FilePrefix='sales_',
                DynamicPath=True)
            transfer.transfer_file_gcs(delete_original_file=True)
        
        @task()
        def debts_transfer():
            transfer = TransferFile(
                PathOrig=default_args['fisiovet_file_path'],
                PathDest='FisioVet/debts/date=',
                FilePrefix='debts_',
                DynamicPath=True)
            transfer.transfer_file_gcs(delete_original_file=True)

        clients_animals_transfer()
        sales_transfer() 
        debts_transfer()

    dbt_source_freshness = BashOperator(
            task_id='dbt_source_freshness',
            bash_command=f'dbt source freshness --profiles-dir {default_args["DBT_PROJECT_DIR"]} --project-dir {default_args["DBT_PROJECT_DIR"]}'
        )
    #dbt source freshness --profiles-dir '/airflow/dags/FisioVet/.dbt' --project-dir '/airflow/dags/FisioVet/.dbt'
    dbt_test = BashOperator(
            task_id='dbt_test',
            bash_command=f'dbt test --profiles-dir {default_args["DBT_PROJECT_DIR"]} --project-dir {default_args["DBT_PROJECT_DIR"]}'
        )
    
    dbt_documentation = BashOperator(
            task_id='dbt_documentation',
            bash_command=f'dbt docs generate --profiles-dir {default_args["DBT_PROJECT_DIR"]} --project-dir {default_args["DBT_PROJECT_DIR"]}'
        )

    dbt_run = BashOperator(
             task_id='dbt_run',
             bash_command=f'dbt run --profiles-dir {default_args["DBT_PROJECT_DIR"]} --project-dir {default_args["DBT_PROJECT_DIR"]}'
         )
    
    end_task = EmptyOperator(
            task_id = 'end_task'
        )   
    
    start_task >> \
    Label("Download dos arquivos") >> fisiovet_downloader() >> \
    Label("Buscar e Normalizar arquivos") >> file_transformation() >> \
    Label("Envio para GCS") >> file_transfer() >> \
    Label("Checa o carregamento dos dados da origem") >> dbt_source_freshness >> \
    Label("Executa os testes do DBT") >> dbt_test >> \
    Label("Gera a documentação do DBT") >> dbt_documentation >> \
    Label("Executa o DBT") >> dbt_run >> \
    end_task

fisiovet_dag()