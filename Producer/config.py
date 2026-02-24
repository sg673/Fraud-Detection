from dataclasses import dataclass
import os
from data_loader import load_merchants


@dataclass
class UserConfig:
    NUM_USERS = 100
    FRAUD_PROBABILITY = 0.05

    COUNTRY_CURRENCY = {"GB": "GBP", "FR": "EUR",
                        "DE": "EUR", "ES": "EUR", "US": "USD"}
    DEVICES = ["mobile", "desktop", "tablet"]
    PAYMENT_METHODS = ["credit_card", "debit_card",
                       "paypal", "apple_pay", "google_pay"]

    # TODO - add ways to generate merchant categories if no data is found
    MERCHANT_CATEGORIES = load_merchants(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "data\\merchants.json"))
    USER_DATA_PATH = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), "data\\profiles.json")


class Config:
    def __init__(self) -> None:
        self.UserConfig = UserConfig()
