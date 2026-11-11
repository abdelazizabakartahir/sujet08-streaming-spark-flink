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
