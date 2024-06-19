from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import BaseOperator
from hook.weather_hook import WeatherHook
from airflow.macros import ds_add

class WeatherOperator(BaseOperator):
    template_fields = ["start_dt"]

    def __init__(self, cities, start_dt, days, **kwargs):
        super().__init__(**kwargs)

        self.cities = cities
        self.start_dt = start_dt
        self.days = days

    def create_file_path(self, city):
        self.file_path = f'/opt/airflow/files/WeatherAPI/{city}/semana={self.start_dt}/dados_'

    def create_parent_folder(self):
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        end_dt = ds_add(self.start_dt,self.days)
        for city in self.cities:
            self.create_file_path(city)
            self.create_parent_folder()

            weather_data = WeatherHook(self.start_dt, end_dt, city)
            data = weather_data.run()
            data.to_csv(self.file_path + 'brutos.csv')
            data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(self.file_path + 'temperaturas.csv')
            data[['datetime', 'description', 'icon']].to_csv(self.file_path + 'condicoes.csv')

        
        

