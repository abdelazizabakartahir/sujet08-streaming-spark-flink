"""
Simulateur de transactions de vente.
Ce script génère un flux continu de messages JSON et les envoie dans un topic Kafka.
- Envoie ~100 msg/s par défaut (configurable via la variable d'environnement RATE).
- Injecte 5% de messages "en retard" (timestamp reculé de 30s) pour tester la robustesse des moteurs.
"""
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

# ==============================================================================
# CONFIGURATION (via variables d'environnement)
# ==============================================================================
# Broker Kafka (défini dans docker-compose.yml comme "kafka:29092")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
# Topic cible pour l'envoi des messages
TOPIC = os.getenv("TOPIC", "sales")
# Débit d'envoi (messages par seconde)
RATE = int(os.getenv("RATE", "100"))
# Pourcentage de messages en retard (par défaut 5%)
LATE_RATIO = float(os.getenv("LATE_RATIO", "0.05"))
# Durée d'exécution en secondes (0 = infini)
DURATION = int(os.getenv("DURATION", "0"))

# Liste des produits et de leurs prix unitaires
PRODUITS = ["laptop", "phone", "tablet", "monitor", "keyboard", "mouse"]
PRIX = {"laptop": 1200, "phone": 800, "tablet": 500,
        "monitor": 300, "keyboard": 80, "mouse": 30}


# ==============================================================================
# FONCTION DE GÉNÉRATION D'UN MESSAGE
# ==============================================================================
def build_message():
    """
    Construit un dictionnaire Python représentant une transaction de vente.
    Gère l'injection de messages en retard (5% du temps).
    """
    produit = random.choice(PRODUITS)
    now = datetime.now(timezone.utc)
    
    # 5% de chance d'injecter un message avec un timestamp reculé de 30 secondes
    if random.random() < LATE_RATIO:
        event_time = now - timedelta(seconds=30)
        late = True
    else:
        event_time = now
        late = False
        
    return {
        "id": str(uuid.uuid4()),                  # Identifiant unique (UUID)
        "produit": produit,                       # Nom du produit
        "prix": PRIX[produit] * random.randint(1, 3), # Prix total (prix unitaire * quantité)
        "quantite": random.randint(1, 3),         # Quantité achetée (1 à 3)
        "event_time": event_time.isoformat(),     # Timestamp de l'événement (ISO 8601)
        "produced_at": now.timestamp(),           # Timestamp d'envoi (pour mesurer la latence)
        "late": late,                             # Indicateur booléen du retard
    }


# ==============================================================================
# FONCTION PRINCIPALE (BOUCLE D'ENVOI)
# ==============================================================================
def main():
    print(">>> DEMARRAGE DU PRODUCTEUR <<<", flush=True)
    
    # Initialisation du producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        # Sérialise le dictionnaire Python en JSON (format UTF-8)
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Réduit la latence en regroupant les messages (batch)
        linger_ms=5,
    )
    
    print(f"[producer] connecting to {KAFKA_BROKER}, topic={TOPIC}, rate={RATE}/s", flush=True)
    
    # Calcul de l'intervalle entre chaque message pour respecter le débit RATE
    interval = 1.0 / RATE
    start = time.time()
    count = 0
    
    try:
        # Boucle infinie (ou limitée par DURATION)
        while True:
            # Vérifie si la durée d'exécution est atteinte
            if DURATION and (time.time() - start) > DURATION:
                break
                
            # Génère et envoie le message
            msg = build_message()
            producer.send(TOPIC, msg)
            count += 1
            
            # Affiche un log tous les 500 messages
            if count % 500 == 0:
                print(f"[producer] sent {count} messages", flush=True)
                
            # Pause pour respecter le débit
            time.sleep(interval)
            
    except KeyboardInterrupt:
        # Arrêt propre si l'utilisateur fait Ctrl+C
        print(f"\n[producer] stopped after {count} messages", flush=True)
    finally:
        # S'assure que tous les messages en attente sont envoyés avant de fermer
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()