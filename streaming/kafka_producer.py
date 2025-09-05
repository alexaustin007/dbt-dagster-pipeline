
"""Simple Kafka producer that publishes random sales events to the 'sales_events' topic"""
import json, random, time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

import uuid

products = ["PROD_101", "PROD_102", "PROD_103", "PROD_104"]
stores = [1, 2, 3, 4, 5]
depts = [1, 2, 3, 4]
transaction_types = ["SALE", "RETURN", "EXCHANGE"]
payment_methods = ["CASH", "CARD", "MOBILE"]

while True:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 100), 2)
    total_amount = round(quantity * unit_price, 2)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow().isoformat(),
        "store_id": random.choice(stores),
        "dept_id": random.choice(depts),
        "product_id": random.choice(products),
        "customer_id": str(uuid.uuid4()),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "transaction_type": random.choice(transaction_types),
        "payment_method": random.choice(payment_methods),
        "promotion_applied": random.choice([True, False])
    }
    producer.send("sales_events", event)
    print("Sent", event)
    time.sleep(1)
