"""StreamFlow streaming job: Kafka -> parse/clean/validate -> console (DB sink in Phase 6)."""
import os

from pyspark.sql import SparkSession

from transformations import process

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")

spark = (
    SparkSession.builder
    .appName("StreamFlow")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

processed = process(raw)

query = (
    processed.select(
        "transaction_id", "customer_id", "amount", "quantity", "event_time",
        "unit_price", "amount_band", "is_valid", "rejection_reason",
    )
    .writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 30)
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()