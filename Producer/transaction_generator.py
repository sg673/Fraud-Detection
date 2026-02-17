from datetime import datetime, timezone
import random
import uuid

class TransactionGenerator:
    def __init__(self, config, users, fraud_simulator):
        self.config = config
        self.users = users
        self.fraud_simulator = fraud_simulator
    
    def generate(self, user_id, is_fraud=False, fraud_type=None):
        profile = self.users[user_id]
        
        merchant = random.choice(list(self.config.UserConfig.MERCHANT_CATEGORIES.keys()))
        merchant_info = self.config.UserConfig.MERCHANT_CATEGORIES[merchant]
        
        amount = max(1, random.gauss(merchant_info["avg"], merchant_info["std"]))
        
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "amount": round(amount, 2),
            "currency": self.config.UserConfig.COUNTRY_CURRENCY[profile["home_country"]],
            "country": profile["home_country"],
            "device": profile["device"],
            "merchant": merchant,
            "merchant_category": merchant_info["category"],
            "payment_method": profile["payment_method"],
            "ip_address": profile["ip_address"],
            "is_fraud_simulated": is_fraud,
        }
        
        if is_fraud and fraud_type != "velocity":
            transaction = self.fraud_simulator.apply_fraud(transaction, profile, fraud_type)
        
        return transaction
