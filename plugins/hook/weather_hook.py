from airflow.providers.http.hooks.http import HttpHook
import pandas as pd

class WeatherHook(HttpHook):

    def __init__(self, start_dt, end_dt, city, key, conn_id=None):
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.city = city
        self.key = key
        self.conn_id = conn_id or "weather_api"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):

        start_dt = self.start_dt
        end_dt = self.end_dt
        city = self.city
        key = self.key

        URL = (f'{self.base_url}/{city}/{start_dt}/{end_dt}?unitGroup=metric&include=days&key={key}&contentType=csv')
        print(f"URL será usada:{URL}")
        return URL

    def get_data(self, URL):
        data = pd.read_csv(URL)

        return data

    def run(self):
        session = self.get_conn()
        URL = self.create_url()

        return self.get_data(URL)
