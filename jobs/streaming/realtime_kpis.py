"""Realtime KPIs: 1-minute windowed aggregates -> ClickHouse.

The speed layer. While bronze ingestion lands raw history, this job keeps
a tiny hot table (`rt.kpis_minute`) that a dashboard can poll: events,
active sessions, purchases, revenue — per minute, updated continuously.

Concepts on display:
  * event time vs processing time — windows use event_ts, not arrival time
  * watermarks — how long we wait for late events before finalizing a window
  * update-mode aggregation + ReplacingMergeTree — late data re-emits a
    window, ClickHouse keeps the freshest version per window (idempotent-ish
    upsert; the last-write-wins pattern)
  * its own consumer group — this job and bronze_ingest read the same topic
    independently, at their own pace
"""

import os

import clickhouse_connect
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
EVENTS_TOPIC = os.environ.get("CLICKSTREAM_TOPIC", "shopstream.events.v1")
CHECKPOINTS = os.environ.get("CHECKPOINT_ROOT", "/checkpoints/realtime_kpis")
CH = {
    "host": os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
    "username": os.environ.get("CLICKHOUSE_USER", "shopstream"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", "shopstream"),
}

spark = SparkSession.builder.appName("realtime_kpis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

avro_schema = requests.get(
    f"{SCHEMA_REGISTRY}/subjects/{EVENTS_TOPIC}-value/versions/latest", timeout=10
).json()["schema"]

client = clickhouse_connect.get_client(**CH)
client.command("CREATE DATABASE IF NOT EXISTS rt")
client.command("""
    CREATE TABLE IF NOT EXISTS rt.kpis_minute (
        window_start DateTime,
        window_end   DateTime,
        events       UInt64,
        sessions     UInt64,
        purchases    UInt64,
        revenue      Float64,
        updated_at   DateTime DEFAULT now()
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY window_start
""")
KPI_COLUMNS = ["window_start", "window_end", "events", "sessions", "purchases", "revenue"]

events = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", EVENTS_TOPIC)
    .option("startingOffsets", "latest")  # a live dashboard needs no history
    .load()
    .select(
        from_avro(
            F.expr("substring(value, 6, length(value) - 5)"),
            avro_schema,
            {"mode": "PERMISSIVE"},
        ).alias("e")
    )
    .filter(F.col("e.event_id").isNotNull())
    .select("e.*")
)

kpis = (
    # Watermark: windows stay open 2 minutes past their end for late events
    # (the generator makes ~2% of events up to 5 min late — those older than
    # the watermark are dropped here; count them against bronze to see it).
    events.withWatermark("event_ts", "2 minutes")
    .groupBy(F.window("event_ts", "1 minute"))
    .agg(
        F.count("*").alias("events"),
        F.approx_count_distinct("session_id").alias("sessions"),
        F.count(F.when(F.col("event_type") == "purchase", True)).alias("purchases"),
        F.coalesce(
            F.sum(F.col("properties").getItem("order_value").cast("double")),
            F.lit(0.0),
        ).alias("revenue"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "events", "sessions", "purchases", "revenue",
    )
)


def write_to_clickhouse(batch_df, batch_id: int) -> None:
    rows = [[r[c] for c in KPI_COLUMNS] for r in batch_df.collect()]
    if rows:
        client.insert("rt.kpis_minute", rows, column_names=KPI_COLUMNS)


query = (
    kpis.writeStream.outputMode("update")  # re-emit windows as late data lands
    .foreachBatch(write_to_clickhouse)
    .option("checkpointLocation", CHECKPOINTS)
    .trigger(processingTime="30 seconds")
    .start()
)
query.awaitTermination()
