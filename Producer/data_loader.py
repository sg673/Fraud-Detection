import json
import os
from user_profiles import generate_profiles

def load_users(profiles_path='profiles.json'):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    filepath = os.path.join(dir_path, profiles_path)
    
    try:
        with open(filepath, "r") as f:
            users = json.load(f)
            if users:
                return users
    except FileNotFoundError:
        pass
    
    return generate_profiles()
