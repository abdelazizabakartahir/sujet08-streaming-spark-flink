"""
Job Spark Structured Streaming : Agrégation des ventes.
Ce script lit un flux de transactions depuis Kafka, applique des fenêtres
temporelles de 10 secondes, calcule le chiffre d'affaires par produit,
et écrit le résultat dans un topic de sortie Kafka.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Récupération des variables d'environnement (définies dans docker-compose.yml)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated")
CHECKPOINT = os.getenv("CHECKPOINT", "/tmp/checkpoints/spark")

# ==============================================================================
# DÉFINITION DU SCHÉMA DES DONNÉES (JSON)
# ==============================================================================
# On définit manuellement le schéma du JSON pour que Spark puisse le lire
# plus rapidement (sans inférence de type qui serait coûteuse en streaming).
schema = StructType([
    StructField("id", StringType()),
    StructField("produit", StringType()),
    StructField("prix", DoubleType()),
    StructField("quantite", IntegerType()),
    StructField("event_time", StringType()),
    StructField("produced_at", DoubleType()),
    StructField("late", BooleanType()),
])

# ==============================================================================
# INITIALISATION DE LA SESSION SPARK
# ==============================================================================
spark = (SparkSession.builder
         .appName("SalesStreaming")
         # On limite le nombre de partitions de shuffle à 4 pour optimiser les performances
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
# On met le niveau de log à WARN pour éviter l'affichage massif des logs INFO
spark.sparkContext.setLogLevel("WARN")

# ==============================================================================
# 1. LECTURE DU FLUX KAFKA (SOURCE)
# ==============================================================================
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BROKER)
       .option("subscribe", INPUT_TOPIC)
       # On commence à lire les nouveaux messages (latest)
       .option("startingOffsets", "latest")
       # Empêche le job de planter si des données sont perdues (ex: offset supprimé)
       .option("failOnDataLoss", "false")
       .load())

# ==============================================================================
# 2. PARSING ET NETTOYAGE DES DONNÉES
# ==============================================================================
parsed = (raw
          # On extrait le JSON contenu dans la colonne "value" (binaire)
          .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
          # On éclate le struct "d" en colonnes individuelles (id, produit, etc.)
          .select("d.*")
          # Conversion du timestamp string en type Timestamp Spark
          .withColumn("event_ts", F.to_timestamp("event_time"))
          .withColumn("produced_ts", F.to_timestamp(F.col("produced_at").cast("double")))
          # Watermark : on tolère les données en retard jusqu'à 10 secondes.
          # Au-delà, les données sont considérées comme trop en retard et ignorées.
          .withWatermark("event_ts", "10 seconds"))

# ==============================================================================
# 3. AGRÉGATION PAR FENÊTRE TEMPORELLE
# ==============================================================================
agg = (parsed
       .groupBy(
           # Fenêtre tumbling de 10 secondes (non chevauchante)
           F.window(F.col("event_ts"), "10 seconds").alias("window"),
           F.col("produit")
       )
       .agg(
           # Calcul du chiffre d'affaires : somme(prix * quantité)
           F.sum(F.col("prix") * F.col("quantite")).alias("ca"),
           # Nombre de transactions
           F.count("*").alias("nb_ventes"),
           # Calcul de la latence moyenne de bout en bout
           F.avg(F.unix_timestamp("event_ts") - F.unix_timestamp("produced_ts")).alias("latence_sec_avg"),
       )
       .select(
           # On extrait les bornes de la fenêtre pour l'affichage
           F.col("window.start").alias("window_start"),
           F.col("window.end").alias("window_end"),
           F.col("produit"),
           F.col("ca"),
           F.col("nb_ventes"),
           F.col("latence_sec_avg"),
       ))

# ==============================================================================
# 4. ÉCRITURE DANS LA CONSOLE (POUR LA DÉMO)
# ==============================================================================
console = (agg.writeStream
           # Mode "update" : n'affiche que les lignes modifiées (gain de place)
           .outputMode("update")
           .format("console")
           .option("truncate", "false") # Ne coupe pas les colonnes
           .option("numRows", "20")
           # Déclenchement toutes les 5 secondes (micro-batch)
           .trigger(processingTime="5 seconds")
           .start())

# ==============================================================================
# 5. ÉCRITURE DANS KAFKA (POUR LA PERSISTANCE)
# ==============================================================================
kafka_out = (agg
             # Convertit le DataFrame en une seule colonne "value" au format JSON
             .selectExpr("CAST(to_json(struct(*)) AS STRING) AS value")
             .writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BROKER)
             .option("topic", OUTPUT_TOPIC)
             # Dossier de sauvegarde des checkpoints (essentiel pour la reprise après panne)
             .option("checkpointLocation", CHECKPOINT)
             .option("failOnDataLoss", "false")
             .outputMode("update")
             .trigger(processingTime="5 seconds")
             .start())

# ==============================================================================
# 6. ATTENTE DE L'ARRÊT
# ==============================================================================
# Le job tourne indéfiniment jusqu'à ce qu'on l'arrête manuellement.
spark.streams.awaitAnyTermination()