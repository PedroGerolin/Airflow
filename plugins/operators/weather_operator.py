from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import BaseOperator
from hook.weather_hook import WeatherHook
from airflow.macros import ds_add

class WeatherOperator(BaseOperator):
    template_fields = ["start_dt", "file_path"]

    def __init__(self, city, start_dt, days, key, file_path, **kwargs):
        super().__init__(**kwargs)

        self.city = city
        self.key = key
        self.start_dt = start_dt
        self.days = days
        self.file_path = file_path

    def create_parent_folder(self):
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()
        end_dt = ds_add(self.start_dt,self.days)

        weather_data = WeatherHook(self.start_dt, end_dt, self.city, self.key)
        data = weather_data.run()
        data.to_csv(self.file_path + 'brutos.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(self.file_path + 'temperaturas.csv')
        data[['datetime', 'description', 'icon']].to_csv(self.file_path + 'condicoes.csv')

