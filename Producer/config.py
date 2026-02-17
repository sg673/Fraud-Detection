from dataclasses import dataclass


@dataclass
class ProducerConfig:
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
