from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    count,
    window,
    lit,
    when,
    sum,
    avg,
    round,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


def write_to_postgres(batch_df, batch_id):
    (
        batch_df
        .write
        .mode("append")
        .jdbc(
            url="jdbc:postgresql://postgres:5432/frauddb",
            table="fraud_velocity_windows",
            properties={
                "user": "fraud_user",
                "password": "fraud_pass",
                "driver": "org.postgresql.Driver"
            },
        )
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
events = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

velocity_agg = (
    events
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        col("user_id"),
        window(col("event_time"), "1 minute")
    )
    .agg(
        count("*").alias("tx_count"),
        sum("amount").alias("sum_amount"),
        avg("amount").alias("avg_amount"),
    )
)

fraud_scores = (
    velocity_agg
    .withColumn(
        "velocity_flag",
        col("tx_count") > lit(5)
    )
    .withColumn(
        "amount_flag",
        col("sum_amount") > lit(1000)
    )
    .withColumn(
        "burst_flag",
        col("tx_count") > lit(10)
    )

    .withColumn(
        "risk_score",
        round(
            when(col("velocity_flag"), 0.4).otherwise(0.0) +
            when(col("amount_flag"), 0.4).otherwise(0.0) +
            when(col("burst_flag"), 0.2).otherwise(0.0),
            2
        )
    )

    .withColumn(
        "risk_level",
        when(col("risk_score") >= 0.8, "HIGH")
        .when(col("risk_score") >= 0.4, "MEDIUM")
        .otherwise("LOW")
    )

    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),

        col("tx_count"),
        col("sum_amount"),
        col("avg_amount"),

        col("velocity_flag"),
        col("amount_flag"),
        col("burst_flag"),

        col("risk_score"),
        col("risk_level"),
    )

)


query = (
    fraud_scores
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

query.awaitTermination()
