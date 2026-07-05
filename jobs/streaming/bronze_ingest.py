"""Streaming ingestion into the lakehouse bronze layer.

Three continuous queries in one Spark application:

  1. clickstream -> lake.bronze.events      (validated Avro, flattened)
  2. bad clickstream messages -> DLQ topic  (poison pills, quarantined)
  3. CDC topics  -> lake.bronze.cdc_raw     (raw Debezium JSON, untouched)

Bronze rules: append-only, keep everything, add ingest metadata, do NOT
clean. Cleaning (dedupe by event_id, typing, flattening CDC envelopes)
belongs to silver — that's phase 4.

Confluent wire format note: each Avro message is [magic 0x00][4-byte
schema id][avro binary]. spark-avro knows nothing about Schema Registry,
so we fetch the schema over SR's REST API once at startup and strip the
5-byte header ourselves — the standard OSS trick, and a good way to
actually understand the wire format.
"""

import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
EVENTS_TOPIC = os.environ.get("CLICKSTREAM_TOPIC", "shopstream.events.v1")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "shopstream.events.dlq")
CHECKPOINTS = os.environ.get("CHECKPOINT_ROOT", "/checkpoints/bronze_ingest")

spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

avro_schema = requests.get(
    f"{SCHEMA_REGISTRY}/subjects/{EVENTS_TOPIC}-value/versions/latest", timeout=10
).json()["schema"]

spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.bronze.events (
        event_id        string,
        event_type      string,
        event_ts        timestamp,
        session_id      string,
        customer_id     bigint,
        device_type     string,
        device_os       string,
        device_browser  string,
        page_url        string,
        referrer        string,
        properties      map<string, string>,
        kafka_partition int,
        kafka_offset    bigint,
        kafka_ts        timestamp,
        ingested_at     timestamp
    ) USING iceberg
    PARTITIONED BY (days(event_ts))
""")
spark.sql("""
    CREATE TABLE IF NOT EXISTS lake.bronze.cdc_raw (
        topic           string,
        key             string,
        value           string,
        kafka_partition int,
        kafka_offset    bigint,
        kafka_ts        timestamp,
        ingested_at     timestamp
    ) USING iceberg
    PARTITIONED BY (topic)
""")

# ---------------------------------------------------------------------------
# Clickstream: validate, split good/bad, land good rows in bronze.events
# ---------------------------------------------------------------------------
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", EVENTS_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

# PERMISSIVE: malformed payloads decode to a null struct instead of killing
# the stream — one poison pill must never stop the pipeline (ask anyone who
# has been paged for exactly that).
decoded = raw.select(
    "*",
    F.expr("substring(value, 1, 1) = X'00'").alias("has_magic_byte"),
    from_avro(
        F.expr("substring(value, 6, length(value) - 5)"),
        avro_schema,
        {"mode": "PERMISSIVE"},
    ).alias("e"),
)
is_valid = F.col("has_magic_byte") & F.col("e.event_id").isNotNull()

good = decoded.filter(is_valid).select(
    F.col("e.event_id").alias("event_id"),
    F.col("e.event_type").alias("event_type"),
    F.col("e.event_ts").alias("event_ts"),
    F.col("e.session_id").alias("session_id"),
    F.col("e.customer_id").alias("customer_id"),
    F.col("e.device.type").alias("device_type"),
    F.col("e.device.os").alias("device_os"),
    F.col("e.device.browser").alias("device_browser"),
    F.col("e.page_url").alias("page_url"),
    F.col("e.referrer").alias("referrer"),
    F.col("e.properties").alias("properties"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("ingested_at"),
)

events_query = (
    good.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/events")
    .trigger(processingTime="30 seconds")
    .toTable("lake.bronze.events")
)

# Bad messages go to the DLQ topic byte-for-byte, so they can be inspected
# and replayed after a fix. Never silently drop data.
dlq_query = (
    decoded.filter(~is_valid)
    .select(F.col("key"), F.col("value"))
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("topic", DLQ_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINTS}/dlq")
    .trigger(processingTime="30 seconds")
    .start()
)

# ---------------------------------------------------------------------------
# CDC: mirror every Debezium topic into one raw bronze table, untouched
# ---------------------------------------------------------------------------
cdc_query = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribePattern", r"cdc\.shopstream\.public\..*")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
    .select(
        F.col("topic"),
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
    )
    .writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINTS}/cdc")
    .trigger(processingTime="30 seconds")
    .toTable("lake.bronze.cdc_raw")
)

spark.streams.awaitAnyTermination()
