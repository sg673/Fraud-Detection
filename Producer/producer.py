from datetime import datetime, timezone
import time
import json
import random
import uuid
import os

from kafka import KafkaProducer
from faker import Faker
from config import Config
from user_profiles import generate_profiles

dir_path = os.path.dirname(os.path.realpath(__file__))

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "transactions"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
fake = Faker()
config = Config()


users = {}
with open(os.path.join(dir_path, 'profiles.json'), "r") as f:
    users = json.load(f)
    print(users)
if not users:
    users = generate_profiles()


def generate_transaction(user_id, is_fraud=False, fraud_type=None):
    profile = users[user_id]

    merchant = random.choice(
        list(config.UserConfig.MERCHANT_CATEGORIES.keys()))
    merchant_info = config.UserConfig.MERCHANT_CATEGORIES[merchant]

    amount = max(1, random.gauss(merchant_info["avg"], merchant_info["std"]))
    country = profile["home_country"]
    device = profile["device"]
    payment_method = profile["payment_method"]
    ip_address = profile["ip_address"]

    if is_fraud:
        if fraud_type == "large_amount":
            amount *= random.uniform(8, 15)
        elif fraud_type == "geo_jump":
            country = random.choice(
                [c for c in config.UserConfig.COUNTRY_CURRENCY.keys() if c != profile["home_country"]])
            ip_address = fake.ipv4()
        elif fraud_type == "velocity":
            for _ in range(random.randint(3, 6)):
                pass
        elif fraud_type == "unusual_time":
            device = random.choice(
                [d for d in config.UserConfig.DEVICES if d != profile["device"]])

    event = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(amount, 2),
        "currency": config.UserConfig.COUNTRY_CURRENCY[country],
        "country": country,
        "device": device,
        "merchant": merchant,
        "merchant_category": merchant_info["category"],
        "payment_method": payment_method,
        "ip_address": ip_address,
        "is_fraud_simulated": is_fraud,
    }

    return event


if __name__ == "__main__":
    pass
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
                transaction = generate_transaction(
                    user_id, is_fraud=True, fraud_type=fraud_type)
                producer.send(TOPIC_NAME, transaction)
                total_transactions += 1
        else:

            transaction = generate_transaction(
                user_id, is_fraud, fraud_type=fraud_type)

            producer.send(TOPIC_NAME, transaction)
            total_transactions += 1
        print(f"\r Sent transaction #{total_transactions}", end="", flush=True)

        time.sleep(random.uniform(0.1, 0.5))
