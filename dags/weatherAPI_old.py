from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.macros import ds_add

with DAG(
        "WeatherAPI_old",
        start_date=pendulum.datetime(2024, 5, 27, tz="UTC"),
        schedule_interval='@daily' #'0 0 * * 1',  # executar toda segunda feira
) as dag:
    task_criar_pasta = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/opt/airflow/files/semana={{data_interval_end.strftime("%Y-%m-%d")}}/"'
    )

    def fn_busca_dados(data_interval_end):
        # Documentação da API usada no projeto
        # https://www.visualcrossing.com/resources/documentation/weather-api/timeline-weather-api/

        city = 'SaoPaulo'
        key = 'QNSRNJVD9U43K2Y4WXYY8F6LK'

        # URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
        #            f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')
        URL = (f'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
               f'{city}/{data_interval_end}/{ds_add(data_interval_end,6)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        dados = pd.read_csv(URL)

        file_path = f'/opt/airflow/files/semana={data_interval_end}/'

        dados.to_csv(file_path + 'dados_brutos.csv')
        dados[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    task_busca_dados = PythonOperator(
        task_id='busca_dados',
        python_callable=fn_busca_dados,
        op_kwargs={'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    task_criar_pasta >> task_busca_dados