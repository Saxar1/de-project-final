import vertica_python
from airflow.models import Variable

def transform_and_load_data():
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