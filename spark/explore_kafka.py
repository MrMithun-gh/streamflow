"""Exploration script: read raw messages from Kafka and print them.
Purpose: prove Spark can talk to Kafka before we add any logic."""
import os

from pyspark.sql import SparkSession

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

spark = (
    SparkSession.builder
    .appName("StreamFlow-Explore")
    .config("spark.sql.shuffle.partitions", "3")   # default is 200, far too many for us
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka gives us binary key/value. Cast to readable strings and keep metadata.
readable = raw.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "partition",
    "offset",
    "timestamp",
)

query = (
    readable.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 10)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()