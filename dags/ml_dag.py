import datetime
from datetime import timedelta

import pandas as pd
import pytz
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
operator_args = {
    'owner': 'dshagarov',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

CURRENT_TIME_ZONE = 'Europe/Moscow'
tz = pytz.timezone(CURRENT_TIME_ZONE)
begin = tz.localize(datetime.datetime(2022, 5, 29, 21, 28, 0, 0))


def download_dataset():
    url = 'https://drive.google.com/file/d/1Tn4XRYCdEK-b7RvMfsVwJV-TWljC6avZ/view?usp=sharing'

    r = requests.get(url)

    file_id = url.split('/')[-2]
    dwn_url = 'https://drive.google.com/uc?id=' + file_id

    df = pd.read_csv(dwn_url)
    df.to_csv('data/dataset.csv')


def read_dataset():
    data = pd.read_csv('data/dataset.csv')
    print('Shape is', data.shape)


with DAG('ml_dag',
         default_args=operator_args,
         description='Pandas user dag',
         schedule_interval=timedelta(days=1),
         start_date=begin,
         catchup=False,
         tags=['my_examples'],
         dagrun_timeout=timedelta(seconds=14)
         ) as dag:
    download_data = PythonOperator(
        task_id='download',
        python_callable=download_dataset
    )

    read_data = PythonOperator(
        task_id='read',
        python_callable=read_dataset
    )

    download_data >> read_data
