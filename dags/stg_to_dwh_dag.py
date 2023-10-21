from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import vertica_python
import os


default_args = {
    'start_date': datetime(2022, 10, 1),
}

dag = DAG(
    'data_transfer_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

def transform_and_load_data():
    vertica_host = str(os.getenv('VERTICA_HOST') or "")
    vertica_port = int(str(os.getenv('VERTICA_PORT') or 0))
    vertica_dbname = str(os.getenv('VERTICA_DBNAME') or "")
    vertica_user = str(os.getenv('VERTICA_USER') or "")
    vertica_password = str(os.getenv('VERTICA_PASSWORD') or "")
    
    # Подключение к Vertica
    vertica_conn_info = {
        'host': vertica_host,
        'port': vertica_port,
        'user': vertica_user,
        'password': vertica_password,
        'database': vertica_dbname
    }
    vertica_conn = vertica_python.connect(**vertica_conn_info)
    vertica_cursor = vertica_conn.cursor()
    
    # Очистка данных от тестовых аккаунтов
    vertica_cursor.execute('DELETE FROM ST23052702__STAGING.transactions WHERE account_id < 0')
    
    # Добавление новой партиции данных за вчерашний день
    yesterday = datetime.now() - timedelta(days=1)
    partition_name = 'partition_' + yesterday.strftime('%Y%m%d')
    vertica_cursor.execute('ALTER TABLE ST23052702__DWH.global_metrics ADD PARTITION %s', (partition_name,))
    
    # Расчет агрегатов
    vertica_cursor.execute('INSERT INTO ST23052702__DWH.global_metrics '
                           'SELECT date_update, currency_from, '
                           'SUM(amount), COUNT(*), '
                           'AVG(COUNT(*)) OVER (PARTITION BY date_update, currency_from), '
                           'COUNT(DISTINCT account_id) '
                           'FROM ST23052702__STAGING.transactions '
                           'GROUP BY date_update, currency_from')
    
    # Закрытие соединения
    vertica_cursor.close()
    vertica_conn.close()

task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=transform_and_load_data,
    dag=dag
)

task