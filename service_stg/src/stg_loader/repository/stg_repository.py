from datetime import datetime

from lib.vertica import VerticaConnect

class StgRepository:
    def __init__(self, db: VerticaConnect) -> None:
        self._db = db

    def currencies_insert(self,
                          date_update: datetime,
                          currency_code: str,
                          currency_code_with: str,
                          currency_with_div: float
                          ) -> None:
        sql = """
                INSERT INTO ST23052702__STAGING.currencies (
                    date_update,
                    currency_code,
                    currency_code_with,
                    currency_with_div
                )
                VALUES (
                    :date_update,
                    :currency_code,
                    :currency_code_with,
                    :currency_with_div
                )
            """
        with self._db.vertica_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, 
                            {
                                'date_update': date_update, 
                                'currency_code': currency_code, 
                                'currency_code_with': currency_code_with, 
                                'currency_with_div': currency_with_div
                            }
                )

    def transactions_insert(self,
                          operation_id: str,
                          account_number_from: int,
                          account_number_to: int,
                          currency_code: int,
                          country: str,
                          status: str,
                          transaction_type: str,
                          amount: int,
                          transaction_dt: datetime
                          ) -> None:
        
        sql = """
                INSERT INTO ST23052702__STAGING.transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                VALUES (
                    :operation_id,
                    :account_number_from,
                    :account_number_to,
                    :currency_code,
                    :country,
                    :status,
                    :transaction_type,
                    :amount,
                    :transaction_dt
                )
            """
        with self._db.vertica_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql,
                        {
                            'operation_id': operation_id,
                            'account_number_from': account_number_from,
                            'account_number_to': account_number_to,
                            'currency_code': currency_code,
                            'country': country,
                            'status': status,
                            'transaction_type': transaction_type,
                            'amount': amount,
                            'transaction_dt': transaction_dt
                        }
                )