"""
Job Flink (PyFlink) : Agrégation des ventes en temps réel.
Ce script lit un flux de transactions depuis Kafka, applique des fenêtres
temporelles de 10 secondes, calcule le chiffre d'affaires par produit,
et écrit le résultat dans un topic de sortie Kafka.
"""
import json
import os
import time
from datetime import datetime

# ==============================================================================
# IMPORTS PYFLINK
# ==============================================================================
from pyflink.common import WatermarkStrategy, Duration, Types
from pyflink.common.time import Time
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
)
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# Récupération des variables d'environnement (définies dans docker-compose.yml)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "sales")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "sales-aggregated-flink")

# ==============================================================================
# 1. INITIALISATION DE L'ENVIRONNEMENT FLINK
# ==============================================================================
env = StreamExecutionEnvironment.get_execution_environment()
# On définit le parallélisme à 2 (2 tâches en parallèle pour le traitement)
env.set_parallelism(2)

# Activation du checkpointing toutes les 5 secondes (5000 ms) pour la tolérance aux pannes.
# Le mode EXACTLY_ONCE garantit que chaque message est traité une seule fois.
env.enable_checkpointing(5000, CheckpointingMode.EXACTLY_ONCE)
# Définition du dossier où seront sauvegardés les checkpoints
env.get_checkpoint_config().set_checkpoint_storage(
    FileSystemCheckpointStorage("file:///tmp/flink-checkpoints")
)

# ==============================================================================
# 2. SOURCE KAFKA (LECTURE DU FLUX)
# ==============================================================================
source = (KafkaSource.builder()
          .set_bootstrap_servers(KAFKA_BROKER)
          .set_topics(INPUT_TOPIC) # Écoute le topic "sales"
          .set_group_id("flink-sales") # Groupe de consommateurs
          # Commence à lire à partir des derniers messages (latest)
          .set_starting_offsets(KafkaOffsetsInitializer.latest())
          # Désérialise les messages Kafka en chaînes de caractères (JSON)
          .set_value_only_deserializer(SimpleStringSchema())
          .build())

# ==============================================================================
# 3. FONCTION DE PARSING
# ==============================================================================
def parse(line):
    """
    Transforme la chaîne JSON en dictionnaire Python.
    Convertit le timestamp 'event_time' (format ISO) en millisecondes (epoch).
    """
    d = json.loads(line)
    event_ts = datetime.fromisoformat(d["event_time"]).timestamp()
    d["event_ts_ms"] = int(event_ts * 1000)
    return json.dumps(d)

# ==============================================================================
# 4. FONCTION D'AGRÉGATION (PROCESS WINDOW FUNCTION)
# ==============================================================================
class WindowAgg(ProcessWindowFunction):
    """
    Cette classe est appelée une fois que la fenêtre de 10 secondes est terminée.
    Elle parcourt tous les éléments de la fenêtre et calcule :
    - le chiffre d'affaires (ca)
    - le nombre de ventes (nb_ventes)
    - la latence moyenne (latence_sec_avg)
    """
    def process(self, key, context, elements):
        ca = 0.0
        nb = 0
        lat_sum = 0.0
        
        # Boucle sur tous les événements de la fenêtre
        for e in elements:
            d = json.loads(e)
            ca += d["prix"] * d["quantite"]
            nb += 1
            lat_sum += time.time() - d["produced_at"]
            
        avg_lat = lat_sum / nb if nb else 0.0
        w = context.window()
        
        # Construction du résultat final (au format JSON)
        result = {
            "window_start": w.start,
            "window_end": w.end,
            "produit": key,
            "ca": ca,
            "nb_ventes": nb,
            "latence_sec_avg": avg_lat,
        }
        yield json.dumps(result)

# ==============================================================================
# 5. SINK KAFKA (ÉCRITURE DU RÉSULTAT)
# ==============================================================================
sink = (KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC) # Écrit dans "sales-aggregated-flink"
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build())

# ==============================================================================
# 6. WATERMARK (GESTION DU TEMPS ET DES RETARDS)
# ==============================================================================
# On configure une watermark avec une tolérance de 10 secondes.
# Cela signifie que Flink attendra 10 secondes avant de fermer une fenêtre,
# afin de traiter les événements qui arrivent en retard (out-of-order).
watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))

# ==============================================================================
# 7. PIPELINE DE TRAITEMENT
# ==============================================================================
# Lecture depuis la source avec la stratégie de watermark
ds = env.from_source(source, watermark_strategy, "kafka-source")

# Chaîne de traitement :
# 1. map(parse) : convertit le JSON
# 2. key_by() : regroupe par produit
# 3. window() : applique une fenêtre tumbling de 10 secondes
# 4. process(WindowAgg()) : calcule les agrégations
result = (ds
          .map(parse, output_type=Types.STRING())
          .key_by(lambda s: json.loads(s)["produit"])
          .window(TumblingEventTimeWindows.of(Time.seconds(10)))
          .process(WindowAgg(), output_type=Types.STRING()))

# Envoi du résultat dans le sink Kafka
result.sink_to(sink)

# ==============================================================================
# 8. EXÉCUTION DU JOB
# ==============================================================================
env.execute("SalesFlinkJob")