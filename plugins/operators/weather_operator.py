from datetime import datetime, timedelta
from pathlib import Path
from airflow.models import BaseOperator
from hook.weather_hook import WeatherHook

class WeatherOperator(BaseOperator):
    template_fields = ["start_dt", "end_dt", "file_path"]

    def __init__(self, city, start_dt, days, key, file_path, **kwargs):
        super().__init__(**kwargs)

        self.city = city
        self.key = key
        self.start_dt = start_dt
        self.days = days
        self.file_path = file_path

        #self.end_dt = '2024-06-20'
        self.end_dt = datetime.strptime('2024-06-14', "%Y-%m-%d") + timedelta(days=self.days)

    def create_parent_folder(self):
        print(f"Path usado:{self.file_path}")
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()

        weather_data = WeatherHook(self.start_dt, self.end_dt.strftime('%Y-%m-%d'), self.city, self.key)
        data = weather_data.run()
        data.to_csv(self.file_path + 'brutos.csv')
        data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(self.file_path + 'temperaturas.csv')
        data[['datetime', 'description', 'icon']].to_csv(self.file_path + 'condicoes.csv')

