from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType

spark = SparkSession.builder \
    .appName("Kafka to Vertica") \
    .getOrCreate()

currencies_schema = StructType([
    StructField("date_update", TimestampType()),
    StructField("currency_code", StringType()),
    StructField("currency_code_with", StringType()),
    StructField("currency_with_div", DecimalType(10, 4))
])

transactions_schema = StructType([
    StructField("operation_id", StringType()),
    StructField("account_number_from", IntegerType()),
    StructField("account_number_to", IntegerType()),
    StructField("currency_code", IntegerType()),
    StructField("country", StringType()),
    StructField("status", StringType()),
    StructField("transaction_type", StringType()),
    StructField("amount", IntegerType()),
    StructField("transaction_dt", TimestampType())
])

def main():
    if 



if __name__ == "__main__":
    main()
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "rc1a-vnj01ua1un60cv7t.mdb.yandexcloud.net:9091") \
    .option("subscribe", "transaction-service-input") \
    .load() \
    .select(from_json(df.value.cast("string"), schema).alias("data"))


df = df.select("data.*")


df = df.withColumn("date_update", df.date_update.cast(TimestampType())) \
    .withColumn("currency_with_div", df.currency_with_div.cast(DecimalType(10, 4)))


df.write \
    .format("jdbc") \
    .option("url", "jdbc:vertica://vertica.tgcloudenv.ru:5433/dwh") \
    .option("dbtable", "ST23052702__STAGING.currencies") \
    .option("user", "st23052702") \
    .option("password", "oEf0V0yqMejSTPS") \
    .mode("append") \
    .save()
