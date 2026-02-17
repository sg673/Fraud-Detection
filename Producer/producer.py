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
fake = Faker()

NUM_USERS = 100
FRAUD_PROBABILITY = 0.05

COUNTRY_CURRENCY = {"GB": "GBP", "FR": "EUR",
                    "DE": "EUR", "ES": "EUR", "US": "USD"}
DEVICES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["credit_card", "debit_card",
                   "paypal", "apple_pay", "google_pay"]

MERCHANT_CATEGORIES = {
    "Amazon": {"category": "retail", "avg": 45, "std": 25},
    "Netflix": {"category": "streaming", "avg": 12, "std": 3},
    "Uber": {"category": "transport", "avg": 18, "std": 8},
    "Spotify": {"category": "streaming", "avg": 10, "std": 2},
    "Apple": {"category": "retail", "avg": 150, "std": 100},
    "Walmart": {"category": "retail", "avg": 65, "std": 30},
    "Starbucks": {"category": "food", "avg": 8, "std": 4},
    "Shell": {"category": "fuel", "avg": 50, "std": 15},
}

users = {}


for user_id in range(1, NUM_USERS + 1):
    country = random.choice(list(COUNTRY_CURRENCY.keys()))
    users[user_id] = {
        "home_country": country,
        "currency": COUNTRY_CURRENCY[country],
        "device": random.choice(DEVICES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "ip_address": fake.ipv4(),
    }


def generate_transaction(user_id, is_fraud=False, fraud_type=None):
    profile = users[user_id]

    merchant = random.choice(list(MERCHANT_CATEGORIES.keys()))
    merchant_info = MERCHANT_CATEGORIES[merchant]

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
                [c for c in COUNTRY_CURRENCY.keys() if c != profile["home_country"]])
            ip_address = fake.ipv4()
        elif fraud_type == "velocity":
            for _ in range(random.randint(3, 6)):
                pass
        elif fraud_type == "unusual_time":
            device = random.choice(
                [d for d in DEVICES if d != profile["device"]])

    event = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(amount, 2),
        "currency": COUNTRY_CURRENCY[country],
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
    print("Starting transaction producer...")
    total_transactions = 0
    while True:
        user_id = random.randint(1, NUM_USERS)
        is_fraud = random.random() < FRAUD_PROBABILITY
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
