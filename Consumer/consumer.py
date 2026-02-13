from pyspark.sql import SparkSession
from threading import Thread
from config import Config
from database import DatabaseWriter
from transformations import parse_kafka_events, compute_velocity_aggregates, compute_user_statistics
from fraud_detection import apply_fraud_flags
from stream_processors import FraudScoreProcessor, UserStatsProcessor


def main():
    """Initialize and run fraud detection streaming pipelines."""
    config = Config()

    spark = SparkSession.builder.appName(  # type: ignore
        "FraudStreaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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
    fraud_scores = apply_fraud_flags(velocity_agg, config.fraud)
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
