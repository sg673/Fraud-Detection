# This file will generate user profiles so the producer will be deterministic
# Running this file will overwrite the existing user data

import json
import random
from faker import Faker
from config import Config

config = Config()


def generate_profiles():
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    users = {}
    for user_id in range(1, config.UserConfig.NUM_USERS + 1):
        country = random.choice(
            list(config.UserConfig.COUNTRY_CURRENCY.keys()))

        users[user_id] = {
            "home_country": country,
            "currency": config.UserConfig.COUNTRY_CURRENCY[country],
            "device": random.choice(config.UserConfig.DEVICES),
            "payment_method": random.choice(config.UserConfig.PAYMENT_METHODS),
            "ip_addresses": [fake.ipv4() for _ in range(random.randint(2, 5))],
        }

    with open(config.UserConfig.USER_DATA_PATH, "w") as f:
        json.dump(users, f, indent=2)

    return users


if __name__ == "__main__":
    generate_profiles()
