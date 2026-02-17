import json
from kafka import KafkaProducer

class KafkaClient:
    def __init__(self, bootstrap_servers, topic_name):
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    
    def send(self, transaction):
        self.producer.send(self.topic_name, transaction)
