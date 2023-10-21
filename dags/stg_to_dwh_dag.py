from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import vertica_python
# import os
from airflow.models import Variable

default_args = {
    'start_date': datetime(2022, 10, 1),
}

dag = DAG(
    'data_transfer_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

def transform_and_load_data():
    # vertica_host = os.getenv('VERTICA_HOST')
    # vertica_port = os.getenv('VERTICA_PORT')
    # vertica_dbname = os.getenv('VERTICA_DBNAME')
    # vertica_user = os.getenv('VERTICA_USER')
    # vertica_password = os.getenv('VERTICA_PASSWORD')
    
    # Подключение к Vertica
    vertica_conn_info = {
        'host': 'vertica.tgcloudenv.ru',
        'port': '5433',
        'user': 'st23052702',
        'password': Variable.get("vertica_pass"),
        'database': 'dwh'
    }
    vertica_conn = vertica_python.connect(**vertica_conn_info)
    vertica_cursor = vertica_conn.cursor()
    
    # Очистка данных от тестовых аккаунтов
    vertica_cursor.execute('DELETE FROM ST23052702__STAGING.transactions WHERE account_number_from < 0')
    
    # Добавление новой партиции данных за вчерашний день
    # yesterday = datetime.now() - timedelta(days=1)
    # partition_name = 'partition_' + yesterday.strftime('%Y%m%d')
    # vertica_cursor.execute('ALTER TABLE ST23052702__DWH.global_metrics ADD PARTITION %s', (partition_name,))
    
    # Расчет агрегатов
    vertica_cursor.execute("""
                            INSERT INTO ST23052702__DWH.global_metrics (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
                            SELECT
                                c.date_update,
                                t.currency_code,
                                SUM(t.amount) / c.currency_with_div AS amount_total,
                                COUNT(t.operation_id) AS cnt_transactions,
                                SUM(t.amount) / COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account,
                                COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
                            FROM
                                ST23052702__STAGING.transactions t
                            JOIN
                                ST23052702__STAGING.currencies c
                            ON
                                t.currency_code = c.currency_code
                            GROUP BY
                                c.date_update,
                                c.currency_with_div,
                                t.currency_code;
                        """
    )

    vertica_conn.commit()
    # Закрытие соединения
    vertica_cursor.close()
    vertica_conn.close()

task = PythonOperator(
    task_id='transform_and_load_data',
    python_callable=transform_and_load_data,
    dag=dag
)

task