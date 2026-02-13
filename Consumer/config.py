from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    url: str = "jdbc:postgresql://postgres:5432/frauddb"
    user: str = "fraud_user"
    password: str = "fraud_pass"
    driver: str = "org.postgresql.Driver"

    def get_properties(self):
        return {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }


@dataclass
class KafkaConfig:
    bootstrap_servers: str = "kafka:29092"
    topic: str = "transactions"
    starting_offsets: str = "latest"


@dataclass
class FraudThresholds:
    velocity_threshold: int = 5
    amount_threshold: int = 1000
    deviation_threshold: float = 3.0
    high_risk_threshold: float = 0.8
    medium_risk_threshold: float = 0.4
    velocity_weight: float = 0.4
    amount_weight: float = 0.4
    deviation_weight: float = 0.2


@dataclass
class StreamConfig:
    velocity_watermark: str = "2 minutes"
    velocity_window: str = "1 minute"
    user_stats_watermark: str = "5 minutes"
    fraud_checkpoint: str = "tmp/checkpoints_fraud_scores"
    user_stats_checkpoint: str = "tmp/checkpoints_user_stats"


class Config:
    def __init__(self):
        self.database = DatabaseConfig()
        self.kafka = KafkaConfig()
        self.fraud = FraudThresholds()
        self.stream = StreamConfig()
