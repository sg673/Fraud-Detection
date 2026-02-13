from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, round, exp, pow as sql_pow
from config import FraudThresholds


def calculate_velocity_probability(df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Calculate probability of fraud based on Poisson distribution for velocity."""
    lambda_param = thresholds.velocity_threshold
    return df.withColumn(
        "velocity_prob",
        when(col("tx_count") > lambda_param,
             1 - exp(-lambda_param * sql_pow(lambda_param, col("tx_count")) /
                     (col("tx_count") + 1))).otherwise(0.0)

    )


def calculate_amount_probability(df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Calculate probability of fraud based on amount deviation."""
    return df.withColumn(
        "amount_prob",
        when(col("avg_transaction_amount") > 0,
             1 / (1 + exp(-0.01 * (col("sum_amount") - thresholds.amount_threshold)))
             ).otherwise(0.0)
    )


def calculate_deviation_probability(df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Calculate probability based on deviation from user's normal behavior."""
    return df.withColumn(
        "amount_deviation_ratio",
        when(col("avg_transaction_amount") > 0,
             col("avg_amount") / col("avg_transaction_amount")).otherwise(1.0)
    ).withColumn(
        "deviation_prob",
        when(col("amount_deviation_ratio") > thresholds.deviation_threshold,
             (col("amount_deviation_ratio") - thresholds.deviation_threshold) /
             (col("amount_deviation_ratio") + 1)).otherwise(0.0)
    )


def enrich_with_risk_score(batch_df: DataFrame, user_stats_df: DataFrame, thresholds: FraudThresholds) -> DataFrame:
    """Enrich fraud flags with user statistics and calculate risk score/level."""
    enriched = batch_df.join(user_stats_df, on="user_id", how="left")

    df_with_probs = (
        calculate_velocity_probability(enriched, thresholds)
        .transform(lambda df: calculate_amount_probability(df, thresholds))
        .transform(lambda df: calculate_deviation_probability(df, thresholds))
    )

    return (
        df_with_probs
        .withColumn(
            "fraud_probability",
            round(
                1 - (1 - col("velocity_prob") * thresholds.velocity_weight) *
                    (1 - col("amount_prob") * thresholds.amount_weight) *
                    (1 - col("deviation_prob") * thresholds.deviation_weight),
                4
            )
        )

        .withColumn(
            "risk_level",
            when(col("fraud_probability") >=
                 thresholds.high_risk_threshold, "HIGH")
            .when(col("fraud_probability") >= thresholds.medium_risk_threshold, "MEDIUM")
            .otherwise("LOW")
        )
        .select(
            "user_id", "window_start", "window_end", "tx_count", "sum_amount", "avg_amount",
            "amount_deviation_ratio", "velocity_prob", "amount_prob", "deviation_prob",
            "fraud_probability", "risk_level"
        )
    )
