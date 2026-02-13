from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)


def get_transaction_schema():
    return StructType([
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
