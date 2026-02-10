from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)

spark = (
    SparkSession.builder
    .appName("FraudStreaming")  # type: ignore
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Kafka source
# -----------------------------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)

# -----------------------------
# Schema
# -----------------------------
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("merchant", StringType()),
    StructField("is_fraud_simulated", BooleanType()),
])

# -----------------------------
# Parse JSON
# -----------------------------
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# -----------------------------
# Output (debug)
# -----------------------------
query = (
    parsed_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
