"""daily_batch — the classic ELT pipeline, one hop at a time.

    extract (Postgres -> Parquet on MinIO)   [dynamically mapped per table]
      -> load (MinIO Parquet -> ClickHouse `raw`)   [mapped, one per extract]
      -> dbt run  (raw -> staging -> marts in ClickHouse)
      -> dbt test

Deliberately simple v1 choices, each of which becomes a later lesson:
  * FULL extracts every run (fine while tables are small). The daily
    snapshots this produces are themselves useful: they let us *see* the
    "snapshot misses intermediate states" problem that CDC (phase 2) fixes.
  * Loads are idempotent per ds: re-running a day drops and reloads that
    day's partition — safe to retry, safe to re-run.
  * ClickHouse infers table schemas from the Parquet files on first load;
    a breaking upstream schema change will break the load. Phase 4 makes
    that failure mode explicit and tested.
"""

import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

TABLES = ["customers", "products", "inventory", "orders", "order_items", "payments"]

PG_DSN = os.environ["SHOPSTREAM_PG_DSN"]
S3_ENDPOINT = os.environ["LAKE_S3_ENDPOINT"]
S3_KEY = os.environ["LAKE_S3_KEY"]
S3_SECRET = os.environ["LAKE_S3_SECRET"]
CH_HOST = os.environ["CLICKHOUSE_HOST"]
CH_USER = os.environ["CLICKHOUSE_USER"]
CH_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]

DBT = (
    "dbt {cmd} --project-dir /opt/dbt/shopstream --profiles-dir /opt/dbt "
    "--target-path $DBT_TARGET_PATH"
)


@dag(schedule="@daily", catchup=False, tags=["shopstream", "phase-1"])
def daily_batch():
    @task
    def extract_table(table: str) -> str:
        """Full extract of one OLTP table to partitioned Parquet on the lake."""
        from datetime import datetime, timezone

        import pandas as pd
        import psycopg
        from airflow.sdk import get_current_context

        # Scheduled runs have a logical date ("ds"); manual triggers in
        # Airflow 3 don't — for those, stamp the partition with today.
        logical_date = get_current_context().get("logical_date")
        ds = (logical_date or datetime.now(timezone.utc)).strftime("%Y-%m-%d")
        with psycopg.connect(PG_DSN) as conn:
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)

        path = f"s3://lake/raw/shopstream/{table}/ds={ds}/{table}.parquet"
        df.to_parquet(
            path,
            index=False,
            storage_options={
                "key": S3_KEY,
                "secret": S3_SECRET,
                "client_kwargs": {"endpoint_url": S3_ENDPOINT},
            },
        )
        print(f"extracted {len(df)} rows from {table} -> {path}")
        return path

    @task
    def load_table(path: str) -> None:
        """Load one Parquet extract into ClickHouse, idempotently per ds.

        ClickHouse reads the lake directly via its s3() table function —
        the warehouse pulls from object storage, nothing streams through
        the orchestrator. Airflow only coordinates.
        """
        import clickhouse_connect

        # path looks like s3://lake/raw/shopstream/<table>/ds=<ds>/<table>.parquet
        table = path.split("/raw/shopstream/")[1].split("/")[0]
        ds = path.split("/ds=")[1].split("/")[0]
        s3_url = path.replace("s3://lake/", f"{S3_ENDPOINT}/lake/")

        client = clickhouse_connect.get_client(
            host=CH_HOST, username=CH_USER, password=CH_PASSWORD
        )
        s3_fn = f"s3('{s3_url}', '{S3_KEY}', '{S3_SECRET}', 'Parquet')"

        client.command("CREATE DATABASE IF NOT EXISTS raw")
        # First load creates the table with a schema inferred from Parquet
        client.command(
            f"CREATE TABLE IF NOT EXISTS raw.{table} "
            f"ENGINE = MergeTree PARTITION BY ds ORDER BY tuple() AS "
            f"SELECT *, '{ds}' AS ds FROM {s3_fn} LIMIT 0"
        )
        # Idempotency: replace this ds's partition wholesale
        client.command(f"ALTER TABLE raw.{table} DROP PARTITION '{ds}'")
        client.command(f"INSERT INTO raw.{table} SELECT *, '{ds}' FROM {s3_fn}")
        count = client.command(
            f"SELECT count() FROM raw.{table} WHERE ds = '{ds}'"
        )
        print(f"loaded {count} rows into raw.{table} (ds={ds})")

    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))

    # Dynamic task mapping: one extract per table; each load consumes its
    # extract's XCom (the s3 path) — a 1:1 mapped chain.
    paths = extract_table.expand(table=TABLES)
    load_table.expand(path=paths) >> dbt_run >> dbt_test


daily_batch()
