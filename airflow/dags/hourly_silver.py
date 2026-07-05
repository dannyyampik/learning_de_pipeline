"""hourly_silver — bronze -> silver, then a quality gate that can say NO.

    silver.events            deduped clickstream (bronze keeps the dups)
    silver.<t>_history       typed change history from the raw CDC feed
    silver.<t>_current       latest state per key (deletes filtered out)
      -> quality_gate        SQL assertions; ANY violation fails the run

Silver rules: typed, deduplicated, still row-level (no aggregation — that's
gold). Tables are rebuilt with CREATE OR REPLACE — Iceberg makes the swap
atomic, so readers never see a half-built table, and it's idempotent by
construction. At real scale you'd MERGE increments instead; the trade-off
is discussed in the phase-4 runbook.

Airflow never touches the data itself: each task sends SQL to the Spark
Connect server over gRPC and waits. Orchestrator coordinates; engine works.
"""

import os

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://spark-connect:15002")

# The CDC tables silver tracks, with the typed columns to pull out of the
# Debezium JSON envelope. (timestamptz columns arrive as ISO-8601 strings
# because the connector uses ZonedTimestamp; numerics as strings because
# of decimal.handling.mode=string.)
CDC_TABLES = {
    "orders": {
        "pk": "order_id",
        "columns": {
            "order_id": "bigint",
            "customer_id": "bigint",
            "status": "string",
            "currency": "string",
            "total_amount": "decimal(12,2)",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },
    "customers": {
        "pk": "customer_id",
        "columns": {
            "customer_id": "bigint",
            "email": "string",
            "full_name": "string",
            "country_code": "string",
            "marketing_opt_in": "boolean",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },
    "products": {
        "pk": "product_id",
        "columns": {
            "product_id": "bigint",
            "sku": "string",
            "name": "string",
            "category": "string",
            "subcategory": "string",
            "unit_price": "decimal(10,2)",
            "is_active": "boolean",
            "created_at": "timestamp",
            "updated_at": "timestamp",
        },
    },
}


def spark_session():
    from pyspark.sql import SparkSession

    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()


@dag(
    schedule="@hourly",
    catchup=False,
    tags=["shopstream", "phase-4"],
    default_args={"retries": 2},
)
def hourly_silver():
    @task
    def create_namespace() -> None:
        """Serialized setup: the REST catalog's backing store doesn't love
        four parallel tasks racing to create the same namespace."""
        spark_session().sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")

    @task
    def silver_events() -> int:
        """Dedupe the clickstream by event_id (at-least-once delivery and
        producer retries make duplicates a WHEN, not an IF)."""
        spark = spark_session()
        spark.sql("""
            CREATE OR REPLACE TABLE lake.silver.events
            USING iceberg PARTITIONED BY (days(event_ts)) AS
            SELECT event_id, event_type, event_ts, session_id, customer_id,
                   device_type, device_os, device_browser,
                   page_url, referrer, properties, ingested_at
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY event_id ORDER BY ingested_at, kafka_offset
                ) AS rn
                FROM lake.bronze.events
            )
            WHERE rn = 1
        """)
        return spark.sql("SELECT count(*) FROM lake.silver.events").collect()[0][0]

    @task
    def silver_cdc(table: str) -> int:
        """Flatten one table's Debezium envelope into typed history + current."""
        spec = CDC_TABLES[table]
        pk, cols = spec["pk"], spec["columns"]
        spark = spark_session()

        # Deletes have after=null; take the key from before so the delete
        # itself is part of the history.
        casts = ",\n                   ".join(
            f"CAST(coalesce(get_json_object(value, '$.after.{c}'), "
            f"get_json_object(value, '$.before.{c}')) AS {t}) AS {c}"
            for c, t in cols.items()
        )
        spark.sql(f"""
            CREATE OR REPLACE TABLE lake.silver.{table}_history USING iceberg AS
            SELECT {casts},
                   get_json_object(value, '$.op') AS op,
                   timestamp_millis(CAST(get_json_object(value, '$.ts_ms') AS bigint))
                       AS changed_at,
                   kafka_offset
            FROM lake.bronze.cdc_raw
            WHERE topic = 'cdc.shopstream.public.{table}'
              AND value IS NOT NULL      -- skip Kafka tombstones
        """)
        out_cols = ", ".join([*cols, "op", "changed_at"])
        spark.sql(f"""
            CREATE OR REPLACE TABLE lake.silver.{table}_current USING iceberg AS
            SELECT {out_cols} FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY {pk} ORDER BY changed_at DESC, kafka_offset DESC
                ) AS rn
                FROM lake.silver.{table}_history
            )
            WHERE rn = 1 AND op != 'd'   -- a delete as latest state = row is gone
        """)
        return spark.sql(
            f"SELECT count(*) FROM lake.silver.{table}_current"
        ).collect()[0][0]

    @task
    def quality_gate() -> None:
        """SQL assertions over silver. Any hit fails the task — and the DAG.

        The pattern (declarative checks between layers, pipeline stops on
        violation) is what Great Expectations / Soda productize; at this
        scale plain SQL keeps it transparent.
        """
        checks = {
            "duplicate event_ids in silver.events": """
                SELECT event_id FROM lake.silver.events
                GROUP BY event_id HAVING count(*) > 1 LIMIT 5
            """,
            "null keys or timestamps in silver.events": """
                SELECT * FROM lake.silver.events
                WHERE event_id IS NULL OR event_ts IS NULL OR session_id IS NULL
                LIMIT 5
            """,
            "purchase events missing order_id": """
                SELECT event_id FROM lake.silver.events
                WHERE event_type = 'purchase' AND properties['order_id'] IS NULL
                LIMIT 5
            """,
            "orders with negative totals": """
                SELECT order_id FROM lake.silver.orders_current
                WHERE total_amount < 0 LIMIT 5
            """,
            # Cross-domain reconciliation: every (sufficiently old) purchase
            # event must exist as an order in the CDC-derived state.
            "purchases with no matching order (>10 min old)": """
                SELECT e.event_id, e.properties['order_id'] AS order_id
                FROM lake.silver.events e
                LEFT JOIN lake.silver.orders_history o
                  ON CAST(e.properties['order_id'] AS bigint) = o.order_id
                WHERE e.event_type = 'purchase'
                  AND e.event_ts < current_timestamp() - INTERVAL 10 MINUTES
                  AND o.order_id IS NULL
                LIMIT 5
            """,
        }
        spark = spark_session()
        violations = {}
        for name, sql in checks.items():
            rows = spark.sql(sql).collect()
            if rows:
                violations[name] = [str(r) for r in rows]
        if violations:
            raise AirflowException(f"quality gate failed: {violations}")
        print(f"quality gate passed: {len(checks)} checks green")

    ns = create_namespace()
    events = silver_events()
    cdc = silver_cdc.expand(table=list(CDC_TABLES))
    ns >> [events, cdc]
    [events, cdc] >> quality_gate()


hourly_silver()
