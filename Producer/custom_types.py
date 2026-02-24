from typing import List, TypedDict


class Merchant(TypedDict):
    name: str
    category: str
    country: str
    merchant_risk_score: float
    average_transaction_amount: float
    standard_deviation_transaction_amount: float


class User(TypedDict):
    home_country: str
    currency: str
    device: str
    payment_method: str
    ip_addresses: List[str]
    account_created: str
