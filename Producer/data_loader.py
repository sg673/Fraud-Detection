import json
import os
from user_profiles import generate_profiles


def read_json(path):
    try:
        with open(path, "r") as f:
            content = json.load(f)
            if content:
                return content
    except FileNotFoundError:
        pass


def load_users(profiles_path):
    try:
        return read_json(profiles_path)
    except FileNotFoundError:
        pass
    return generate_profiles()


def load_merchants(merchants_path):
    try:
        return read_json(merchants_path)
    except FileNotFoundError:
        pass
    return
