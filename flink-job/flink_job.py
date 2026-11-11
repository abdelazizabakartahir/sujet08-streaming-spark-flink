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
