from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, round
from config import FraudThresholds


def apply_fraud_flags(df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Apply velocity and amount fraud flags based on thresholds."""
    return (
        df
        .withColumn("velocity_flag", col("tx_count") > lit(thresholds.velocity_threshold))
        .withColumn("amount_flag", col("sum_amount") > lit(thresholds.amount_threshold))
        .select(
            col("user_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("tx_count"),
            col("sum_amount"),
            col("avg_amount"),
            col("velocity_flag"),
            col("amount_flag"),
        )
    )


def enrich_with_risk_score(batch_df: DataFrame, user_stats_df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Enrich fraud flags with user statistics and calculate risk score/level."""
    enriched = batch_df.join(user_stats_df, on="user_id", how="left")

    return (
        enriched
        .withColumn(
            "amount_deviation_ratio",
            when(col("avg_transaction_amount") != 0,
                 col("avg_amount") / col("avg_transaction_amount")).otherwise(0)
        )
        .withColumn("deviation_flag", col("amount_deviation_ratio") > thresholds.deviation_threshold)
        .withColumn(
            "risk_score",
            round(
                when(col("velocity_flag"), thresholds.velocity_weight).otherwise(0.0) +
                when(col("amount_flag"), thresholds.amount_weight).otherwise(0.0) +
                when(col("deviation_flag"),
                     thresholds.deviation_weight).otherwise(0.0),
                2
            )
        )
        .withColumn(
            "risk_level",
            when(col("risk_score") >= thresholds.high_risk_threshold, "HIGH")
            .when(col("risk_score") >= thresholds.medium_risk_threshold, "MEDIUM")
            .otherwise("LOW")
        )
        .select(
            "user_id", "window_start", "window_end", "tx_count", "sum_amount", "avg_amount",
            "velocity_flag", "amount_flag", "amount_deviation_ratio", "deviation_flag",
            "risk_score", "risk_level"
        )
    )
