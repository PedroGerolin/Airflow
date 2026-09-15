from airflow.providers.http.hooks.http import HttpHook
import pandas as pd

class WeatherHook(HttpHook):

    def __init__(self, start_dt, end_dt, city, conn_id=None):
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.city = city
        self.conn_id = conn_id or "weather_api"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self, conn):

        start_dt = self.start_dt
        end_dt = self.end_dt
        city = self.city
        extra_key = conn.extra_dejson['key']

        url = (f'{conn.host}/{city}/{start_dt}/{end_dt}?unitGroup=metric&include=days&key={extra_key}&contentType=csv')
        print(f"URL será usada:{url}")
        return url

    def get_data(self, url):
        data = pd.read_csv(url)

        return data

    def run(self):
        conn = self.get_connection(self.conn_id)
        url = self.create_url(conn)

        return self.get_data(url)
