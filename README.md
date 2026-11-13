# Sujet 08 — Traitement des données en temps réel : Spark, Flink et Kafka

**Master Data Science & Génie Logiciel — Université Alioune Diop de Bambey**  
**Auteur :** ABDEL-AZIZ ABAKAR TAHIR  
**Année :** 2026

---

## 📌 Description du projet

Ce projet est une étude comparative de trois moteurs de streaming : **Spark Structured Streaming**, **Apache Flink** et **Kafka Streams**. Il inclut :
- Une veille technologique complète.
- Une expérimentation pratique conteneurisée avec Docker.
- Un benchmark de performance (latence, débit).
- Une simulation de panne pour tester la tolérance aux pannes.

Le cas d'usage simulé est un flux de **transactions de vente** (produits, prix, quantités, timestamps). Les moteurs calculent le chiffre d'affaires par produit sur des **fenêtres tumbling de 10 secondes**.

---

## 🏗️ Architecture

Le projet utilise **Docker Compose** pour orchestrer les services suivants :

| Service | Rôle | Port |
|:---|:---|:---|
| **Kafka** (KRaft) | Broker de messages | `9092` |
| **Kafka UI** | Visualisation des topics et messages | `8080` |
| **Spark Master/Worker** | Cluster de traitement Spark | `8081` (UI) |
| **Flink JobManager/TaskManager** | Cluster de traitement Flink | `8082` (UI) |
| **Producer** | Simulateur de transactions (Python) | - |

---

## 🚀 Lancer le projet

### Prérequis
- **Docker Desktop** (8 Go de RAM alloués minimum)
- **Git**

### Étape 1 : Démarrer l'infrastructure
```bash
docker compose up -d kafka kafka-ui spark-master spark-worker flink-jobmanager flink-taskmanager
Étape 2 : Créer les topics Kafka
bash
docker exec kafka kafka-topics --create --topic sales --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic sales-aggregated --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic sales-aggregated-flink --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
Étape 3 : Lancer le producteur, Spark et Flink
bash
docker compose up -d producer spark-job flink-job
Étape 4 : Visualiser les résultats
Kafka UI : http://localhost:8080

Spark UI : http://localhost:8081

Flink UI : http://localhost:8082

📊 Benchmark
Pour lancer les tests de charge (100, 1000 et 5000 messages/seconde) :

bash
# Test à 100 msg/s pendant 60 secondes
docker compose run --rm -e RATE=100 -e DURATION=60 producer

# Test à 1000 msg/s pendant 60 secondes
docker compose run --rm -e RATE=1000 -e DURATION=60 producer

# Test à 5000 msg/s pendant 60 secondes
docker compose run --rm -e RATE=5000 -e DURATION=60 producer
Les résultats sont détaillés dans rapport/benchmark.md.

📁 Structure du projet
text
sujet08-streaming/
├── docker-compose.yml       # Orchestration des services
├── README.md                # Ce fichier
├── producer/                # Simulateur de transactions
├── spark-job/               # Job Spark Structured Streaming
├── flink-job/               # Job Apache Flink
├── checkpoints/             # Dossiers de sauvegarde (générés)
├── monitoring/              # Documentation sur la supervision (optionnel)
└── rapport/                 # Rapport, présentation et captures
    ├── Rapport_Sujet08_Streaming_ABDEL-AZIZ_ABAKAR_TAHIR.pdf
    ├── Presentation_Sujet08.pdf
    ├── benchmark.md
    └── captures/            # Captures d'écran (7 figures)
📝 Résultats et conclusions
Spark : Très performant à faible charge (<1000 msg/s), mais sature à 5000 msg/s (warning falling behind).

Flink : Job correctement soumis et maintenu en RUNNING, mais le sink Kafka n'a pas pu être validé (problème de classpath PyFlink).

Reprise après panne Spark : Réussie depuis le checkpoint, sans perte de données.

Pour plus de détails, consultez le rapport complet : rapport/Rapport_Sujet08_Streaming_ABDEL-AZIZ_ABAKAR TAHIR.pdf.