from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from fetch_data import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015-09-01),
    'email': ['mutumakimathi5@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay': timedelta(minutes=5),    
}

dag = DAG (
    'citi_bike_data_pipeline',
    default_args = default_args,
    description = 'A simple data pipeline for fetching and storing Citi Bike data',
    schedule_interval = timedelta(months = 1),   
)

t1 = PythonOperator(
    task_id = 'fetch_data',
    python_callable = main,
    dag = dag,
)

t1