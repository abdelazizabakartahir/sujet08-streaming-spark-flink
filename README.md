# Sujet 08 - Streaming Spark / Flink / Kafka

## Prerequis
- Docker Desktop (6 Go RAM minimum)
- Git

## Lancer le projet

### 1. Demarrer l'infrastructure
docker compose up -d kafka kafka-ui spark-master spark-worker flink-jobmanager flink-taskmanager

### 2. Creer les topics Kafka
docker exec kafka kafka-topics --create --topic sales --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic sales-aggregated --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic sales-aggregated-flink --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

### 3. Lancer producer, Spark, Flink
docker compose up -d producer spark-job flink-job

## Interfaces
- Kafka UI : http://localhost:8080
- Spark UI : http://localhost:8081
- Flink UI : http://localhost:8082

## Benchmark
docker compose run --rm -e RATE=100 -e DURATION=60 producer
docker compose run --rm -e RATE=1000 -e DURATION=60 producer
docker compose run --rm -e RATE=5000 -e DURATION=60 producer

## Resultats
Voir rapport/rapport.pdf et rapport/benchmark.md
