from contextlib import contextmanager
from typing import Generator

import vertica_python

class VerticaConnect:
    def __init__(self, host: str, port: int, db_name: str, user: str, pw: str) -> None:
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.pw = pw

    @contextmanager
    def vertica_connection(self) -> Generator[vertica_python.Connection, None, None]:
        conn = vertica_python.connect(
            host=self.host,
            port=self.port,
            database=self.db_name,
            user=self.user,
            password=self.pw
        )
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()