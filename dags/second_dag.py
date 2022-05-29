import datetime
from datetime import timedelta

import pytz
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
operator_args = {
    'owner': 'dshagarov',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

CURRENT_TIME_ZONE = 'Europe/Moscow'
tz = pytz.timezone(CURRENT_TIME_ZONE)
begin = tz.localize(datetime.datetime(2022, 5, 29, 21, 28, 0, 0))

with DAG('sleeping_dag',
         default_args=operator_args, description='Sleeper',
         schedule_interval=timedelta(seconds=30),
         start_date=begin,
         catchup=False,
         tags=['my_examples'],
         dagrun_timeout=timedelta(seconds=14)
         ) as dag:
    op1 = BashOperator(
        task_id='sleep',
        bash_command='sleep 10'
    )

    op2 = PythonOperator(
        task_id='print_time',
        python_callable=lambda: print('Hello after sleep!')
    )

    op1 >> op2
