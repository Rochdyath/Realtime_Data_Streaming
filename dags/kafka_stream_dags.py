from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG

from stream_data import stream_data

default_args = {
    'owner': 'dyath',
    'start_date': datetime(2024, 3, 30)
}

with DAG('user_automation', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id ='stream_data_from_kafka',
        python_callable=stream_data
    )