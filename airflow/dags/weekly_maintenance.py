"""weekly_maintenance — the chores that keep a lakehouse healthy.

Streaming writes commit every ~30 seconds, and each commit is a snapshot
plus a handful of small Parquet files. Left alone, that means thousands of
tiny files (slow scans) and unbounded metadata. Real lakehouse teams
schedule exactly these three jobs from day one:

  * compact       rewrite small data files into fewer, bigger ones
  * expire        drop old snapshots (bounds metadata + storage; also your
                  time-travel retention policy)
  * archive_dlq   sweep the dead-letter topic into an Iceberg table where
                  it can be inspected with SQL, joined, and kept forever
"""

import os

from airflow.sdk import dag, task

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")
KAFKA = "kafka:29092"
DLQ_TOPIC = "shopstream.events.dlq"

BRONZE_TABLES = ["bronze.events", "bronze.cdc_raw"]
SILVER_TABLES = [
    "silver.events",
    "silver.orders_history", "silver.orders_current",
    "silver.customers_history", "silver.customers_current",
    "silver.products_history", "silver.products_current",
]


def spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()


@dag(schedule="@weekly", catchup=False, tags=["shopstream", "phase-4"])
def weekly_maintenance():
    @task
    def compact() -> dict:
        """Rewrite small files (streaming leaves many) into ~128MB ones."""
        spark = spark_session()
        results = {}
        for t in BRONZE_TABLES:
            row = spark.sql(
                f"CALL lake.system.rewrite_data_files(table => '{t}')"
            ).collect()[0]
            results[t] = f"rewrote {row[0]} files into {row[1]}"
        print(results)
        return results

    @task
    def expire_snapshots() -> dict:
        """Keep 7 days / at least 5 snapshots of history — that's our
        time-travel retention. Expired snapshots free their data files."""
        from datetime import datetime, timedelta, timezone

        cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        spark = spark_session()
        results = {}
        for t in BRONZE_TABLES + SILVER_TABLES:
            if not spark.catalog.tableExists(f"lake.{t}"):
                results[t] = "skipped (does not exist yet)"
                continue
            row = spark.sql(f"""
                CALL lake.system.expire_snapshots(
                    table => '{t}',
                    older_than => TIMESTAMP '{cutoff}',
                    retain_last => 5
                )
            """).collect()[0]
            results[t] = f"deleted {row[0]} data files"
        print(results)
        return results

    @task
    def archive_dlq() -> int:
        """Land the DLQ in the lake. Kafka retention eventually deletes the
        topic's messages; the archive is forever, queryable, and joinable
        with whatever context you need to debug the producer."""
        spark = spark_session()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
        # Batch read of the whole topic; small by construction (bad messages
        # are the exception — if this table is big you have a producer fire).
        (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA)
            .option("subscribe", DLQ_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr(
                "CAST(key AS STRING) AS key",
                "value",                       # keep the poison bytes as-is
                "partition AS kafka_partition",
                "offset AS kafka_offset",
                "timestamp AS kafka_ts",
                "current_timestamp() AS archived_at",
            )
            .writeTo("lake.bronze.dlq_archive")
            .createOrReplace()
        )
        return spark.sql(
            "SELECT count(*) FROM lake.bronze.dlq_archive"
        ).collect()[0][0]

    compact() >> expire_snapshots() >> archive_dlq()


weekly_maintenance()
