import json
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer
from stg_loader.repository import StgRepository

class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = 100

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            if msg["object_type"] == "TRANSACTION":
                self._stg_repository.transactions_insert(
                    msg["payload"]['operation_id'],
                    msg["payload"]['account_number_from'],
                    msg["payload"]['account_number_to'],
                    msg["payload"]['currency_code'],
                    msg["payload"]['country'],
                    msg["payload"]['status'],
                    msg["payload"]['transaction_type'],
                    msg["payload"]['amount'],
                    msg["payload"]['transaction_dt']
                )
            elif msg["object_type"] == "CURRENCY":
                self._stg_repository.currencies_insert(
                    msg["payload"]['date_update'],
                    msg["payload"]['currency_code'],
                    msg["payload"]['currency_code_with'],
                    msg["payload"]['currency_with_div']
                )
        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")