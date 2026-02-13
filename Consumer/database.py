from pyspark.sql import DataFrame, SparkSession
from config import DatabaseConfig


class DatabaseWriter:
    """Handles JDBC operations for reading and writing to PostgreSQL."""

    def __init__(self, spark: SparkSession, config: DatabaseConfig):
        self.spark = spark
        self.config = config

    def read_user_statistics(self) -> DataFrame:
        """Read user statistics table and cache for batch processing."""
        return (
            self.spark.read
            .jdbc(
                url=self.config.url,
                table="user_statistics",
                properties=self.config.get_properties()
            )
            .cache()
        )

    def write_to_table(self, df: DataFrame, table: str, mode: str = "append"):
        """Write DataFrame to specified PostgreSQL table."""
        (
            df.write
            .mode(mode)
            .jdbc(
                url=self.config.url,
                table=table,
                properties=self.config.get_properties()
            )
        )

    def upsert_user_statistics(self, df: DataFrame):
        """Upsert user statistics using temporary table."""
        if df.isEmpty():
            return

        try:
            existing = self.spark.read.jdbc(
                url=self.config.url,
                table="user_statistics",
                properties=self.config.get_properties()
            )
            merged = df.alias("new").join(
                existing.alias("old"),
                on="user_id",
                how="outer"
            ).selectExpr(
                "coalesce(new.user_id, old.user_id) as user_id",
                "coalesce(old.total_transactions, 0) + coalesce(new.total_transactions, 0) as total_transactions",
                "coalesce(old.total_amount, 0) + coalesce(new.total_amount, 0) as total_amount",
                """
            CASE 
                WHEN (coalesce(old.total_transactions, 0) + coalesce(new.total_transactions, 0)) = 0 
                THEN 0
                ELSE 
                    (coalesce(old.total_amount, 0) + coalesce(new.total_amount, 0)) /
                    (coalesce(old.total_transactions, 0) + coalesce(new.total_transactions, 0))
            END as avg_transaction_amount
            """,

                "greatest(coalesce(old.max_transaction_amount, 0), coalesce(new.max_transaction_amount, 0)) as max_transaction_amount",
                "greatest(coalesce(old.distinct_locations, 0), coalesce(new.distinct_locations, 0)) as distinct_locations",

                "current_timestamp() as last_updated"
            )
        except:
            merged = df

        self.write_to_table(merged, "user_statistics", "overwrite")
