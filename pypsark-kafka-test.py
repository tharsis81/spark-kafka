import numpy as np
import pandas as pd
import os
import json
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 pyspark-shell'
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import time

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

spark = SparkSession \
    .builder \
    .appName("KafkaStream") \
    .getOrCreate()

df = spark.read.format("kafka").options(**options).load()

query = df.write.format("console").save()
time.sleep(10)
query.stop()






