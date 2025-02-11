from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import BaseOperator
from hook.weather_hook import WeatherHook
from airflow.macros import ds_add
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os

class WeatherOperator(BaseOperator):
    template_fields = ["start_dt"]

    def __init__(self, cities, start_dt, days, **kwargs):
        super().__init__(**kwargs)

        self.cities = cities
        self.start_dt = start_dt
        self.days = days

    def create_file_path(self, city):
        self.file_path = f'/opt/airflow/files/WeatherAPI/{city}/semana={self.start_dt}/'

    def create_parent_folder(self, full_path):
        Path(full_path).parent.mkdir(parents=True, exist_ok=True)

    def transfer_file_gcs(self, data_folder, gcs_path, **kwargs):
        data_folder = self.file_path
        bucket_name = 'gerolin_etl'
        gcs_conn_id = 'google_cloud_default'

        csv_files = [file for file in os.listdir(data_folder) if file.endswith('dados_brutos.csv')]

        for file in csv_files:
            local_file_path = os.path.join(data_folder, file)
            gcs_file_path = gcs_path + file

            print(f"Arquivo será usada:{local_file_path}")

            upload_to_gcs = LocalFilesystemToGCSOperator(
                task_id='upload_to_gcs',
                src=local_file_path,
                dst=gcs_file_path,
                bucket=bucket_name,
                gcp_conn_id=gcs_conn_id
            )
            upload_to_gcs.execute(context=kwargs)


    def execute(self, context):
        end_dt = ds_add(self.start_dt,self.days)
        for city in self.cities:
            
            self.create_file_path(city)
            filename_full = self.file_path + 'dados_brutos.csv'
            filename_temperature = self.file_path + 'dados_temperaturas.csv'
            filename_conditions = self.file_path + 'dados_condicoes.csv'
            self.create_parent_folder(filename_full)

            weather_data = WeatherHook(self.start_dt, end_dt, city)
            data = weather_data.run()
            data.to_csv(filename_full)
            data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(filename_temperature)
            data[['datetime', 'description', 'icon']].to_csv(filename_conditions)

            self.transfer_file_gcs(data_folder=self.file_path, gcs_path=f'WeatherAPI/city={city}/semana={self.start_dt}/')

        
        

