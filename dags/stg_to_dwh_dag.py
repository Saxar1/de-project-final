from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from vertica_script import transform_and_load_data


default_args = {
    'start_date': datetime(2022, 10, 1),
}

dag = DAG(
    'data_transfer_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=transform_and_load_data,
    dag=dag
)

task