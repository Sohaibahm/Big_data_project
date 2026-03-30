import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

countries = ["FR", "DE", "ES", "RU", "IR", "IT"]
customers = ["client_01", "client_02", "client_03", "client_04"]

i = 1
while True:
    msg = {
        "transaction_id": f"tx_{i:06d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": random.choice(customers),
        "amount": round(random.uniform(10, 10000), 2),
        "currency": "EUR",
        "country": random.choice(countries),
        "merchant_id": f"shop_{random.randint(1, 10):02d}",
        "channel": random.choice(["ONLINE", "POS"]),
        "status": random.choice(["APPROVED", "DECLINED"]),
    }

    producer.send("transactions", msg)
    print("→ Sent:", msg)
    i += 1
    time.sleep(1)  # 1 transaction par seconde
