from pyspark.sql import DataFrame
from config import Config
from database import DatabaseWriter
from fraud_detection import enrich_with_risk_score


class FraudScoreProcessor:
    def __init__(self, db_writer: DatabaseWriter, config: Config):
        self.db_writer = db_writer
        self.config = config

    def process_batch(self, batch_df: DataFrame, batch_id: int):
        """Enrich batch with user stats and write to fraud_velocity_windows table."""

        user_stats_df = self.db_writer.read_user_statistics()
        enriched_df = enrich_with_risk_score(
            batch_df, user_stats_df, self.config.fraud)
        self.db_writer.write_to_table(
            enriched_df, "fraud_velocity_windows", "append")


class UserStatsProcessor:
    def __init__(self, db_writer: DatabaseWriter):
        self.db_writer = db_writer

    def process_batch(self, batch_df: DataFrame, batch_id: int):
        """Write user statistics batch to user_statistics table."""

        self.db_writer.write_to_table(batch_df, "user_statistics", "overwrite")
