curl -X POST https://order-gen-service.sprint9.tgcloudenv.ru/project/register_kafka \
-H 'Content-Type: application/json; charset=utf-8' \
--data-binary @- << EOF
{
    "student": "saxarok",
    "kafka_connect":{
        "host": "rc1a-vnj01ua1un60cv7t.mdb.yandexcloud.net",
        "port": 9091,
        "topic": "transaction-service-input",
        "producer_name": "producer_consumer",
        "producer_password": "saxarok1"
    }
}
EOF

kafkacat -C \
         -b rc1a-vnj01ua1un60cv7t.mdb.yandexcloud.net:9091 \
         -t transaction-service-input \
         -X security.protocol=SASL_SSL \
         -X sasl.mechanism=SCRAM-SHA-512 \
         -X sasl.username="producer_consumer" \
         -X sasl.password="saxarok1" \
         -X ssl.ca.location=/home/saxarok/Desktop/CA.pem -Z -K: