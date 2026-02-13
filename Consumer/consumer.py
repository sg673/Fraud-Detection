from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from threading import Thread
from config import Config
from database import DatabaseWriter
from transformations import parse_kafka_events, compute_velocity_aggregates, compute_user_statistics
from stream_processors import FraudScoreProcessor, UserStatsProcessor


def main():
    """Initialize and run fraud detection streaming pipelines."""
    config = Config()

    spark = SparkSession.builder.appName(  # type: ignore
        "FraudStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    db_writer = DatabaseWriter(spark, config.database)
    fraud_processor = FraudScoreProcessor(db_writer, config)
    user_stats_processor = UserStatsProcessor(db_writer)

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka.bootstrap_servers)
        .option("subscribe", config.kafka.topic)
        .option("startingOffsets", config.kafka.starting_offsets)
        .load()
    )

    events = parse_kafka_events(kafka_df)

    velocity_agg = compute_velocity_aggregates(
        events, config.stream.velocity_watermark, config.stream.velocity_window
    )
    fraud_scores = velocity_agg.select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("tx_count"),
        col("sum_amount"),
        col("avg_amount")
    )
    user_stats = compute_user_statistics(
        events, config.stream.user_stats_watermark)

    fraud_query = (
        fraud_scores.writeStream
        .foreachBatch(fraud_processor.process_batch)
        .outputMode("append")
        .option("checkpointLocation", config.stream.fraud_checkpoint)
        .start()
    )

    user_stats_query = (
        user_stats.writeStream
        .foreachBatch(user_stats_processor.process_batch)
        .outputMode("complete")
        .option("checkpointLocation", config.stream.user_stats_checkpoint)
        .start()
    )

    Thread(target=fraud_query.awaitTermination).start()
    user_stats_query.awaitTermination()


if __name__ == "__main__":
    main()
