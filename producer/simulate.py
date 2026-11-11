"""
Simulateur de transactions de vente.
- Envoie ~100 msg/s par defaut (configurable via RATE)
- Injecte 5% de messages "en retard" (timestamp recule de 30s)
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC = os.getenv("TOPIC", "sales")
RATE = int(os.getenv("RATE", "100"))
LATE_RATIO = float(os.getenv("LATE_RATIO", "0.05"))
DURATION = int(os.getenv("DURATION", "0"))

PRODUITS = ["laptop", "phone", "tablet", "monitor", "keyboard", "mouse"]
PRIX = {"laptop": 1200, "phone": 800, "tablet": 500,
        "monitor": 300, "keyboard": 80, "mouse": 30}


def build_message():
    produit = random.choice(PRODUITS)
    now = datetime.now(timezone.utc)
    if random.random() < LATE_RATIO:
        event_time = now - timedelta(seconds=30)
        late = True
    else:
        event_time = now
        late = False
    return {
        "id": str(uuid.uuid4()),
        "produit": produit,
        "prix": PRIX[produit] * random.randint(1, 3),
        "quantite": random.randint(1, 3),
        "event_time": event_time.isoformat(),
        "produced_at": now.timestamp(),
        "late": late,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
    )
    print(f"[producer] connecting to {KAFKA_BROKER}, topic={TOPIC}, rate={RATE}/s")
    interval = 1.0 / RATE
    start = time.time()
    count = 0
    try:
        while True:
            if DURATION and (time.time() - start) > DURATION:
                break
            msg = build_message()
            producer.send(TOPIC, msg)
            count += 1
            if count % 500 == 0:
                print(f"[producer] sent {count} messages")
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n[producer] stopped after {count} messages")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
