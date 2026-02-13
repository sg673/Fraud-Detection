from datetime import datetime, timezone
import time
import json
import random
import uuid

from kafka import KafkaProducer
from faker import Faker

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

NUM_USERS = 100
FRAUD_PROBABILITY = 0.05

COUNTRIES = ["GB", "FR", "DE", "ES", "US"]
DEVICES = ["mobile", "desktop", "tablet"]
MERCHANTS = ["Amazon", "Netflix", "Uber", "Spotify", "Apple"]

users = {}

for user_id in range(1, NUM_USERS + 1):
    users[user_id] = {
        "home_country": random.choice(COUNTRIES),
        "device": random.choice(DEVICES),
        "avg_amount": random.uniform(20, 80),
        "std_amount": random.uniform(5, 20),
    }


def generate_transaction(user_id):
    profile = users[user_id]
    is_fraud = random.random() < FRAUD_PROBABILITY

    amount = max(
        1,
        random.gauss(profile["avg_amount"], profile["std_amount"]),
    )

    country = profile["home_country"]
    device = profile["device"]

    if is_fraud:
        fraud_type = random.choice(["burst", "large_amount", "geo_jump"])

        # TODO - implement burst fraud by generating multiple transactions in a short time window
        if fraud_type == "large_amount":
            amount *= random.uniform(5, 10)

        elif fraud_type == "geo_jump":
            country = random.choice(
                [c for c in COUNTRIES if c != profile["home_country"]]
            )

    event = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(amount, 2),
        "currency": "GBP",
        "country": country,
        "device": device,
        "merchant": random.choice(MERCHANTS),
        "is_fraud_simulated": is_fraud,
    }

    return event


if __name__ == "__main__":

    print("Starting transaction producer...")
    total_transactions = 0
    while True:
        user_id = random.randint(1, NUM_USERS)
        transaction = generate_transaction(user_id)

        producer.send(TOPIC_NAME, transaction)
        total_transactions += 1
        print("\r Sent transaction #{}".format(
            total_transactions), end="", flush=True)
        # print(transaction)

        time.sleep(random.uniform(0.1, 0.5))
