from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import BaseOperator 
from hook.weather_hook import WeatherHook
from modules.converter import Converter
from modules.transfer import TransferFile
from airflow.macros import ds_add 
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
            data.to_csv(filename_full, index=False)
            data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(filename_temperature)
            data[['datetime', 'description', 'icon']].to_csv(filename_conditions)

            converter = Converter('csv','orc',self.file_path,'dados_brutos.csv')
            converter.convert()

            transfer = TransferFile(PathOrig=self.file_path,PathDest=f'WeatherAPI/city={city}/semana={self.start_dt}/')
            transfer.transfer_file_gcs()
            

        
        

