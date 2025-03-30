import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task, dag
from modules.fisiovet_downloader import fisioVetDownloader

@dag(
        dag_id="fisiovet_scrap",
        start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
        schedule_interval=None,
        catchup=False,
)
def fisiovet_dag():    

    start_task = EmptyOperator(
            task_id = 'start_task'
        )  

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

    
    end_task = EmptyOperator(
            task_id = 'end_task'
        )   
    
    start_task >> \
    download_clients_file() >> download_sales_file() >> \
    end_task

fisiovet_dag()