import json
import os
from user_profiles import generate_profiles


def load_users(profiles_path):
    try:
        with open(profiles_path, "r") as f:
            users = json.load(f)
            if users:
                return users
    except FileNotFoundError:
        pass

    return generate_profiles()
