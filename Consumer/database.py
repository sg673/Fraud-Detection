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
