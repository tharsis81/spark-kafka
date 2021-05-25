import numpy as np
import pandas as pd
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

# context
# sc = SparkContext("local", "KafkaTestApp")
# sc.setLogLevel("WARN")

# ssc = StreamingContext(sc, 1)

#security.protocol=SASL_PLAINTEXT -X sasl.mechanism=SCRAM-SHA-512
# reading (not in stream mode) (writing in console for debug)

#all configs in options 
options = {
   "kafka.bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
   "kafka.group.id": "production-playerblob-v1-enriched-development-clickhouse-group",
   "subscribe": "production-playerblob-v1-enriched",
   "kafka.sasl.mechanism": "SCRAM-SHA-512",
   "kafka.security.protocol" : "SASL_PLAINTEXT",
   "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.scram.ScramLoginModule required username="service-analytics-clickhouse-development" password="pv2NHQz2M3l2";',
   "failOnDataLoss": "false"
}

# options = {
#     "kafka.bootstrap.servers": "localhost:9055",
#     "group.id": "production-playerblob-v1-enriched-ha-clickhouse-group",
#     "subscribe": "production-playerblob-v1-enriched",
#     "kafka.sasl.token.mechanism": "PLAIN",
#     "kafka.security.protocol" : "SASL_SSL",
#     "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="service-analytics-clickhouse-development" password="pv2NHQz2M3l2";',
#     "failOnDataLoss": "false"
# }

spark = SparkSession \
    .builder \
    .appName("KafkaStream") \
    .getOrCreate()

# df = spark \
#   .read \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("subscribe", "production-playerblob-v1-enriched") \
#   .option("group.id", "production-playerblob-v1-enriched-development-clickhouse-group") \
#   .option("startingOffsets", "earliest") \
#   .option("failOnDataLoss", "false") \
#   .option("spark.kafka.clusters.${cluster}.security.protocol", "SASL_PLAINTEXT") \
#   .option("spark.kafka.clusters.${cluster}.sasl.token.mechanism", "SCRAM-SHA-512") \
#   .load()


df = spark.read.format("kafka").options(**options).load()

query = df.write.format("console").save()
time.sleep(10)
query.stop()

# reading (in stream mode) (writing in parquet), max 10 retries
# options_stream = {
#     "kafka.bootstrap.servers": "localhost:9055",
#     "group.id": "production-playerblob-v1-enriched-ha-clickhouse-group",
#     "subscribe": "production-playerblob-v1-enriched",
#     "kafka.sasl.mechanism": "SCRAM-SHA-512",
#     "kafka.security.protocol" : "SASL_PLAINTEXT",
#     "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="service-analytics-clickhouse-development" password="pv2NHQz2M3l2";',
#     "failOnDataLoss": "false"
# }

# df = spark.readStream.format("kafka").options(**options_stream).load().writeStream.format("parquet").




