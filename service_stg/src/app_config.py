import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.vertica import VerticaConnect


class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST') or "")
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')) or 0)
        self.kafka_consumer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "")
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "")
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP') or "")
        self.kafka_consumer_topic = str(os.getenv('KAFKA_SOURCE_TOPIC') or "")
        self.kafka_producer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "")
        self.kafka_producer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "")
        self.kafka_producer_topic = str(os.getenv('KAFKA_DESTINATION_TOPIC') or "")

        self.vertica_host = str(os.getenv('VERTICA_HOST') or "")
        self.vertica_port = int(str(os.getenv('VERTICA_PORT') or 0))
        self.vertica_dbname = str(os.getenv('VERTICA_DBNAME') or "")
        self.vertica_user = str(os.getenv('VERTICA_USER') or "")
        self.vertica_password = str(os.getenv('VERTICA_PASSWORD') or "")

    def kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    def vertica_db(self) -> VerticaConnect:
        return VerticaConnect(
            self.vertica_host,
            self.vertica_port,
            self.vertica_dbname,
            self.vertica_user,
            self.vertica_password
        )
