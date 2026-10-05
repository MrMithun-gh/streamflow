"""Pure transformation functions: DataFrame in, DataFrame out. No Kafka or DB here."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType,
)

# The contract for what a transaction message looks like.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("quantity", IntegerType()),
    StructField("timestamp", StringType()),   # converted to a real timestamp in cleaning
    StructField("payment_method", StringType()),
])


def parse_kafka_messages(raw: DataFrame) -> DataFrame:
    """Kafka bytes -> typed columns. Keeps raw_value so we can store rejected messages."""
    as_text = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    parsed = as_text.withColumn("data", F.from_json("raw_value", TRANSACTION_SCHEMA))
    return parsed.select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        F.col("data").isNull().alias("is_malformed"),   # null struct = unparseable JSON
        F.col("data.*"),                                # expand struct into columns
    )


def clean_transactions(df: DataFrame) -> DataFrame:
    """Fix formatting. Changes values but does not judge validity."""
    return (
        df.withColumn("transaction_id", F.trim("transaction_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("category", F.initcap(F.trim("category")))
        .withColumn("payment_method",
                    F.coalesce(F.lower(F.trim("payment_method")), F.lit("unknown")))
        .withColumn("event_time", F.to_timestamp("timestamp"))   # bad string -> null
        .drop("timestamp")
    )


def _blank(col_name: str):
    """True if a string column is null or empty."""
    return F.col(col_name).isNull() | (F.length(F.col(col_name)) == 0)


def add_validation(df: DataFrame) -> DataFrame:
    """Judge each record. First failing rule wins. Adds rejection_reason and is_valid."""
    reason = (
        F.when(F.col("is_malformed"), "malformed_json")
        .when(_blank("transaction_id"), "null_transaction_id")
        .when(_blank("customer_id"), "missing_customer_id")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "invalid_amount")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("event_time").isNull(), "invalid_timestamp")
        .otherwise(F.lit(None).cast("string"))
    )
    return (
        df.withColumn("rejection_reason", reason)
        .withColumn("is_valid", F.col("rejection_reason").isNull())
    )


def add_derived_fields(df: DataFrame) -> DataFrame:
    """Add useful columns. Only meaningful for valid rows; null-safe for the rest."""
    return (
        df.withColumn("unit_price", F.round(F.col("amount") / F.col("quantity"), 2))
        .withColumn(
            "amount_band",
            F.when(F.col("amount") >= 5000, "high")
            .when(F.col("amount") >= 1000, "medium")
            .when(F.col("amount").isNotNull(), "low"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def process(raw: DataFrame) -> DataFrame:
    """Full pipeline: parse -> clean -> validate -> derive."""
    return add_derived_fields(add_validation(clean_transactions(parse_kafka_messages(raw))))