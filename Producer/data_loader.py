import json
from typing import Any, Dict, Optional
from Producer.types import Merchant, User
from user_profiles import generate_profiles


def read_json(path: str) -> Optional[Any]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def load_users(profiles_path: str) -> Dict[str, User]:
    return read_json(profiles_path) or generate_profiles()


def load_merchants(merchants_path: str) -> Dict[str, Merchant]:
    merchants = read_json(merchants_path)
    if merchants is None:
        raise FileNotFoundError(f"Merchants file not found: {merchants_path}")
    else:
        return {merchant["name"]: merchant for merchant in merchants}
