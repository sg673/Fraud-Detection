from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    from_json, col, count, window, sum, avg, max,
    approx_count_distinct, current_timestamp
)
from schemas import get_transaction_schema


def parse_kafka_events(kafka_df: DataFrame) -> DataFrame:
    """Parse JSON transaction events from Kafka and add event_time column."""
    schema = get_transaction_schema()
    return (
        kafka_df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
    )


def compute_velocity_aggregates(events: DataFrame, watermark: str, window_duration: str) -> DataFrame:
    """Compute windowed transaction velocity metrics per user."""
    return (
        events
        .withWatermark("event_time", watermark)
        .groupBy(col("user_id"), window(col("event_time"), window_duration))
        .agg(
            count("*").alias("tx_count"),
            sum("amount").alias("sum_amount"),
            avg("amount").alias("avg_amount"),
        )
    )


def compute_user_statistics(events: DataFrame, watermark: str) -> DataFrame:
    """Compute aggregate user statistics across all transactions."""
    return (
        events
        .withWatermark("event_time", watermark)
        .groupBy("user_id")
        .agg(
            count("*").alias("total_transactions"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_transaction_amount"),
            max("amount").alias("max_transaction_amount"),
            approx_count_distinct("country").alias("distinct_locations")
        )
        .withColumn("last_updated", current_timestamp())
    )
