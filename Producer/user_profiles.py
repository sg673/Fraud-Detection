# This file will generate user profiles so the producer will be deterministic
# Running this file will overwrite the existing user data

import json
import random
from faker import Faker
import os
dir_path = os.path.dirname(os.path.realpath(__file__))

NUM_USERS = 100

COUNTRY_CURRENCY = {"GB": "GBP", "FR": "EUR",
                    "DE": "EUR", "ES": "EUR", "US": "USD"}
DEVICES = ["mobile", "desktop", "tablet"]
PAYMENT_METHODS = ["credit_card", "debit_card",
                   "paypal", "apple_pay", "google_pay"]


def generate_profiles():
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

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

    with open(os.path.join(dir_path, 'profiles.json'), "w") as f:
        json.dump(users, f, indent=2)

    print(f"Generated {NUM_USERS} user profiles and saved to profiles.json")


if __name__ == "__main__":
    generate_profiles()
