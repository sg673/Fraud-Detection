import time
import random
from config import Config
from kafka_client import KafkaClient
from data_loader import load_users
from transaction_generator import TransactionGenerator
from fraud_simulator import FraudSimulator

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions"


def main():
    config = Config()
    users = load_users(config.UserConfig.USER_DATA_PATH)

    kafka_client = KafkaClient(KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME)
    fraud_simulator = FraudSimulator(config)
    transaction_gen = TransactionGenerator(config, users, fraud_simulator)

    print("Starting transaction producer...")
    total_transactions = 0

    while True:
        user_id = random.randint(1, config.UserConfig.NUM_USERS)
        is_fraud = random.random() < config.UserConfig.FRAUD_PROBABILITY
        fraud_type = None

        if is_fraud:
            fraud_type = random.choice(
                ["large_amount", "geo_jump", "velocity", "unusual_time"])

        if fraud_type == "velocity":
            for _ in range(random.randint(3, 6)):
                transaction = transaction_gen.generate(
                    user_id, True, fraud_type)
                kafka_client.send(transaction)
                total_transactions += 1
        else:
            transaction = transaction_gen.generate(
                user_id, is_fraud, fraud_type)
            kafka_client.send(transaction)
            total_transactions += 1

        print(f"\r Sent transaction #{total_transactions}", end="", flush=True)
        time.sleep(random.uniform(0.1, 0.5))


if __name__ == "__main__":
    main()
