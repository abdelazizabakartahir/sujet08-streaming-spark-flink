# =====================================================
# Script de génération complète du projet Sujet 08
# =====================================================

$ErrorActionPreference = "Stop"

Write-Host ">>> Creation des dossiers..." -ForegroundColor Cyan
$dirs = @("producer","spark-job","flink-job","monitoring","rapport","checkpoints","checkpoints\spark","checkpoints\flink","rapport\captures")
foreach ($d in $dirs) { New-Item -ItemType Directory -Path $d -Force | Out-Null }

Write-Host ">>> Ecriture des fichiers..." -ForegroundColor Cyan

# ---------- .gitignore ----------
@"
__pycache__/
*.pyc
.venv/
.env
checkpoints/
.flink/
*.log
.DS_Store
spark-warehouse/
"@ | Out-File -FilePath ".gitignore" -Encoding utf8

# ---------- docker-compose.yml ----------
@'
version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 10

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      kafka:
        condition: service_healthy

  spark-master:
    image: bitnami/spark:3.5
    container_name: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
    ports:
      - "8081:8080"
      - "7077:7077"
    volumes:
      - ./spark-job:/opt/spark-jobs
      - ./checkpoints/spark:/tmp/checkpoints

  spark-worker:
    image: bitnami/spark:3.5
    container_name: spark-worker
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    depends_on:
      - spark-master
    volumes:
      - ./spark-job:/opt/spark-jobs

  spark-job:
    build: ./spark-job
    container_name: spark-job
    environment:
      KAFKA_BROKER: kafka:29092
      INPUT_TOPIC: sales
      OUTPUT_TOPIC: sales-aggregated
      CHECKPOINT: /tmp/checkpoints/spark
    volumes:
      - ./spark-job:/opt/spark-jobs
      - ./checkpoints/spark:/tmp/checkpoints/spark
    command: >
      /opt/bitnami/spark/bin/spark-submit
      --master spark://spark-master:7077
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
      /opt/spark-jobs/spark_streaming.py
    depends_on:
      - spark-master
      - kafka

  flink-jobmanager:
    image: flink:1.18-scala_2.12
    container_name: flink-jobmanager
    command: jobmanager
    ports:
      - "8082:8081"
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        state.backend: rocksdb
        state.checkpoints.dir: file:///tmp/flink-checkpoints
        execution.checkpointing.interval: 5000
    volumes:
      - ./flink-job:/opt/flink/usrlib
      - ./checkpoints/flink:/tmp/flink-checkpoints

  flink-taskmanager:
    image: flink:1.18-scala_2.12
    container_name: flink-taskmanager
    command: taskmanager
    depends_on:
      - flink-jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 2
        state.backend: rocksdb
        state.checkpoints.dir: file:///tmp/flink-checkpoints
    volumes:
      - ./flink-job:/opt/flink/usrlib
      - ./checkpoints/flink:/tmp/flink-checkpoints

  flink-job:
    build: ./flink-job
    container_name: flink-job
    environment:
      KAFKA_BROKER: kafka:29092
      INPUT_TOPIC: sales
      OUTPUT_TOPIC: sales-aggregated-flink
    volumes:
      - ./flink-job:/opt/flink/usrlib
      - ./checkpoints/flink:/tmp/flink-checkpoints
      - ./flink-job/flink-sql-connector-kafka-3.1.0-1.18.jar:/opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar
    command: >
      flink run -d -py /opt/flink/usrlib/flink_job.py
    depends_on:
      - flink-jobmanager
      - flink-taskmanager
      - kafka

  producer:
    build: ./producer
    container_name: producer
    environment:
      KAFKA_BROKER: kafka:29092
      TOPIC: sales
      RATE: 100
      LATE_RATIO: 0.05
    depends_on:
      kafka:
        condition: service_healthy
'@ | Out-File -FilePath "docker-compose.yml" -Encoding utf8

# ---------- producer/requirements.txt ----------
"kafka-python==2.0.2" | Out-File -FilePath "producer\requirements.txt" -Encoding utf8

# ---------- producer/Dockerfile ----------
@"
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY simulate.py .
CMD ["python", "simulate.py"]
"@ | Out-File -FilePath "producer\Dockerfile" -Encoding utf8

# ---------- producer/simulate.py ----------
@'
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
'@ | Out-File -FilePath "producer\simulate.py" -Encoding utf8

# ---------- spark-job/Dockerfile ----------
@"
FROM bitnami/spark:3.5
USER root
RUN pip install --no-cache-dir kafka-python==2.0.2
USER 1001
WORKDIR /opt/spark-jobs
"@ | Out-File -FilePath "spark-job\Dockerfile" -Encoding utf8

# ---------- spark-job/spark_streaming.py ----------
@'
"""
Job Spark Structured Streaming : sales aggregation.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated")
CHECKPOINT = os.getenv("CHECKPOINT", "/tmp/checkpoints/spark")

schema = StructType([
    StructField("id", StringType()),
    StructField("produit", StringType()),
    StructField("prix", DoubleType()),
    StructField("quantite", IntegerType()),
    StructField("event_time", StringType()),
    StructField("produced_at", DoubleType()),
    StructField("late", BooleanType()),
])

spark = (SparkSession.builder
         .appName("SalesStreaming")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BROKER)
       .option("subscribe", INPUT_TOPIC)
       .option("startingOffsets", "latest")
       .load())

parsed = (raw
          .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
          .select("d.*")
          .withColumn("event_ts", F.to_timestamp("event_time"))
          .withColumn("produced_ts", F.to_timestamp(F.col("produced_at").cast("double")))
          .withWatermark("event_ts", "10 seconds"))

agg = (parsed
       .groupBy(
           F.window(F.col("event_ts"), "10 seconds").alias("window"),
           F.col("produit")
       )
       .agg(
           F.sum(F.col("prix") * F.col("quantite")).alias("ca"),
           F.count("*").alias("nb_ventes"),
           F.avg(F.unix_timestamp("event_ts") - F.unix_timestamp("produced_ts")).alias("latence_sec_avg"),
       )
       .select(
           F.col("window.start").alias("window_start"),
           F.col("window.end").alias("window_end"),
           F.col("produit"),
           F.col("ca"),
           F.col("nb_ventes"),
           F.col("latence_sec_avg"),
       ))

console = (agg.writeStream
           .outputMode("update")
           .format("console")
           .option("truncate", "false")
           .option("numRows", "20")
           .trigger(processingTime="5 seconds")
           .start())

kafka_out = (agg
             .selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BROKER)
             .option("topic", OUTPUT_TOPIC)
             .option("checkpointLocation", CHECKPOINT)
             .outputMode("update")
             .trigger(processingTime="5 seconds")
             .start())

spark.streams.awaitAnyTermination()
'@ | Out-File -FilePath "spark-job\spark_streaming.py" -Encoding utf8

# ---------- flink-job/requirements.txt ----------
"apache-flink==1.18.0" | Out-File -FilePath "flink-job\requirements.txt" -Encoding utf8

# ---------- flink-job/Dockerfile ----------
@"
FROM flink:1.18-scala_2.12
USER root
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir apache-flink==1.18.0
USER flink
WORKDIR /opt/flink/usrlib
"@ | Out-File -FilePath "flink-job\Dockerfile" -Encoding utf8

# ---------- flink-job/flink_job.py ----------
@'
"""
Job Flink (PyFlink) : sales aggregation.
"""
import json
import os
import time
from datetime import datetime

from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated-flink")

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(2)
env.enable_checkpointing(5000, CheckpointingMode.EXACTLY_ONCE)
env.get_checkpoint_config().set_checkpoint_storage_dir("file:///tmp/flink-checkpoints")

source = (KafkaSource.builder()
          .set_bootstrap_servers(KAFKA_BROKER)
          .set_topics(INPUT_TOPIC)
          .set_group_id("flink-sales")
          .set_starting_offsets(KafkaOffsetsInitializer.latest())
          .set_value_only_deserializer(SimpleStringSchema())
          .build())


def parse(line):
    d = json.loads(line)
    event_ts = datetime.fromisoformat(d["event_time"]).timestamp()
    d["event_ts_ms"] = int(event_ts * 1000)
    return json.dumps(d)


class WindowAgg(ProcessWindowFunction):
    def process(self, key, context, elements):
        ca = 0.0
        nb = 0
        lat_sum = 0.0
        for e in elements:
            d = json.loads(e)
            ca += d["prix"] * d["quantite"]
            nb += 1
            lat_sum += time.time() - d["produced_at"]
        avg_lat = lat_sum / nb if nb else 0.0
        w = context.window()
        result = {
            "window_start": w.start,
            "window_end": w.end,
            "produit": key,
            "ca": ca,
            "nb_ventes": nb,
            "latence_sec_avg": avg_lat,
        }
        yield json.dumps(result)


sink = (KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build())

ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

result = (ds
          .map(parse, output_type=Types.STRING())
          .key_by(lambda s: json.loads(s)["produit"])
          .window(TumblingEventTimeWindows.of(Duration.of_seconds(10)))
          .process(WindowAgg(), output_type=Types.STRING()))

result.sink_to(sink)
env.execute("SalesFlinkJob")
'@ | Out-File -FilePath "flink-job\flink_job.py" -Encoding utf8

# ---------- monitoring/README.md ----------
"# Monitoring`n`nDossier optionnel pour Prometheus/Grafana. Non requis pour le rendu." | Out-File -FilePath "monitoring\README.md" -Encoding utf8

# ---------- rapport/benchmark.md ----------
@"
# Resultats du Benchmark

## Environnement
- Machine : [CPU, RAM]
- Docker Desktop version : [X]
- Kafka 7.6 (KRaft), Spark 3.5, Flink 1.18

## Resultats

| Debit (msg/s) | Spark latence avg (s) | Flink latence avg (s) |
|:---:|:---:|:---:|
| 100 | [A REMPLIR] | [A REMPLIR] |
| 1000 | [A REMPLIR] | [A REMPLIR] |
| 5000 | [A REMPLIR] | [A REMPLIR] |

## Reprise apres panne
- Spark : [temps de reprise, perte de donnees oui/non]
- Flink : [temps de reprise, perte de donnees oui/non]

## Captures
Voir dossier rapport/captures/
"@ | Out-File -FilePath "rapport\benchmark.md" -Encoding utf8

# ---------- rapport/rapport.md ----------
@"
# Rapport de veille technologique - Sujet 08

## Traitement des donnees en temps reel : Spark Structured Streaming, Apache Flink et Kafka Streams

### 1. Introduction, contexte et problematique
[A REDIGER]

### 2. Methodologie de veille et criteres de selection des sources
[A REDIGER]

### 3. Presentation des outils et comparaison fonctionnelle
[A REDIGER]

### 4. Protocole experimental, environnement et donnees
[A REDIGER]

### 5. Resultats, analyse critique et limites
[A REDIGER]

### 6. Recommandations, conclusion et perspectives
[A REDIGER]

### 7. Bibliographie et annexes
- Spark : https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Flink : https://nightlies.apache.org/flink/flink-docs-release-1.18/
- Kafka Streams : https://kafka.apache.org/documentation/streams/
"@ | Out-File -FilePath "rapport\rapport.md" -Encoding utf8

# ---------- README.md racine ----------
@"
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
"@ | Out-File -FilePath "README.md" -Encoding utf8

Write-Host ""
Write-Host ">>> Telechargement du JAR connecteur Kafka Flink..." -ForegroundColor Cyan
$jarUrl = "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
$jarDest = "flink-job\flink-sql-connector-kafka-3.1.0-1.18.jar"
Invoke-WebRequest -Uri $jarUrl -OutFile $jarDest
Write-Host "   JAR telecharge : $jarDest" -ForegroundColor Green

Write-Host ""
Write-Host ">>> Init Git..." -ForegroundColor Cyan
git init | Out-Null
git add . | Out-Null
git commit -m "Initial commit: Sujet 08 streaming" | Out-Null

Write-Host ""
Write-Host "=================================================" -ForegroundColor Green
Write-Host " PROJET GENERE AVEC SUCCES !" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Fichiers crees :" -ForegroundColor Yellow
Get-ChildItem -Recurse -File | Where-Object { $_.FullName -notmatch '\\\.git\\' } | Select-Object -ExpandProperty FullName
