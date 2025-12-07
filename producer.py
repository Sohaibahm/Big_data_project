"""
Producer Kafka - Simulation de transactions financières en streaming
-------------------------------------------------------------------

Ce script génère des transactions aléatoires et les envoie
en continu dans un topic Kafka.

Dépendances :
    pip install kafka-python

Lancer Kafka avant d'exécuter ce script :
    docker-compose up -d

Créer le topic :
    kafka-topics.sh --create \
        --bootstrap-server localhost:9092 \
        --replication-factor 1 \
        --partitions 3 \
        --topic transactions

Exécution :
    python producer.py
"""

from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime

# ====================================================================
# 1. Configuration du producteur Kafka
# ====================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Si Docker : "kafka:9092"
KAFKA_TOPIC = "transactions"
SEND_INTERVAL_SECONDS = 1  # envoie une transaction toutes les 1 seconde


# ====================================================================
# 2. Génération d'une transaction aléatoire
# ====================================================================

def generate_transaction():
    """Crée une transaction aléatoire sous forme de dictionnaire Python."""
    
    users = ["U001", "U002", "U003", "U004", "U005"]
    merchants = ["Amazon", "Carrefour", "Uber", "Fnac", "Decathlon"]
    countries = ["FR", "DE", "ES", "IT", "NL"]
    channels = ["WEB", "MOBILE", "POS"]
    currencies = ["EUR", "USD"]

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "amount": round(random.uniform(1, 5000), 2),
        "currency": random.choice(currencies),
        "timestamp": datetime.utcnow().isoformat(),
        "merchant": random.choice(merchants),
        "country": random.choice(countries),
        "channel": random.choice(channels)
    }


# ====================================================================
# 3. Initialisation du Producer Kafka
# ====================================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # JSON → bytes
)


# ====================================================================
# 4. Boucle principale d’envoi des transactions
# ====================================================================

def main():
    print(f"[INFO] Envoi en continu de transactions dans Kafka topic '{KAFKA_TOPIC}'...")
    print("[CTRL+C pour arrêter]\n")
    
    while True:
        tx = generate_transaction()

        # Envoi dans Kafka
        producer.send(KAFKA_TOPIC, value=tx)
        producer.flush()  # permet de s'assurer que le message est envoyé

        print("[ENVOYÉ] ", tx)

        time.sleep(SEND_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[STOP] Producteur arrêté proprement.")
