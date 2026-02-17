# This file will generate user profiles so the producer will be deterministic
# Running this file will overwrite the existing user data

import json
import random
from faker import Faker
import os
from config import Config
dir_path = os.path.dirname(os.path.realpath(__file__))

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
            "ip_address": fake.ipv4(),
        }

    with open(os.path.join(dir_path, 'profiles.json'), "w") as f:
        json.dump(users, f, indent=2)

    return users


if __name__ == "__main__":
    generate_profiles()
