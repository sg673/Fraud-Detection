import random
from faker import Faker

class FraudSimulator:
    def __init__(self, config):
        self.config = config
        self.fake = Faker()
    
    def apply_fraud(self, transaction_data, profile, fraud_type):
        if fraud_type == "large_amount":
            transaction_data["amount"] *= random.uniform(8, 15)
        
        elif fraud_type == "geo_jump":
            transaction_data["country"] = random.choice(
                [c for c in self.config.UserConfig.COUNTRY_CURRENCY.keys() 
                 if c != profile["home_country"]]
            )
            transaction_data["currency"] = self.config.UserConfig.COUNTRY_CURRENCY[transaction_data["country"]]
            transaction_data["ip_address"] = self.fake.ipv4()
        
        elif fraud_type == "unusual_time":
            transaction_data["device"] = random.choice(
                [d for d in self.config.UserConfig.DEVICES 
                 if d != profile["device"]]
            )
        
        return transaction_data
