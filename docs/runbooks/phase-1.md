# Phase 1 — Batch ELT: Airflow → Lake → ClickHouse → dbt → Metabase

**What you build:** the classic "modern data stack" batch pipeline. Every
night (or on demand), Airflow extracts the OLTP tables to Parquet on the
lake, ClickHouse ingests them straight from object storage, dbt turns raw
snapshots into a star schema, and Metabase puts dashboards on top.

**What you learn:** orchestration (DAGs, dynamic task mapping, XComs,
idempotent re-runs), EL vs T, partitioned object storage layout, warehouse
loading patterns (`s3()` table function, partition replacement), dbt
(sources, staging vs marts, macros, tests), and the difference between an
OLTP row store and an OLAP column store — by using both on the same data.

---

## Run it

```bash
make up-batch        # phase 0 services + ClickHouse + Airflow + Metabase
make ps              # wait until airflow is healthy (first boot takes ~1 min)
make trigger-daily   # kick the DAG without waiting for midnight
make demo-olap       # raw loads + marts + last 7 days of revenue
```

| What | Where | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | none (all-admins mode, local only) |
| ClickHouse HTTP | `localhost:8123` | `shopstream` / `shopstream` |
| Metabase | http://localhost:3000 | created on first visit |
| (plus everything from [phase 0](phase-0.md)) | | |

> Coming from phase 0 with an old Postgres volume? Run `make nuke` once —
> Airflow's metadata database is created by a new Postgres init script,
> which only runs on a fresh volume.

## The data's journey

1. **Extract** (`extract_table`, one mapped task per table): full
   `SELECT *` from Postgres → `s3://lake/raw/shopstream/<table>/ds=<date>/`.
   Hive-style `ds=` partitioning is the lake convention everything else
   (Spark, Trino, ClickHouse) understands.
2. **Load** (`load_table`): ClickHouse pulls the Parquet directly off MinIO
   with its `s3()` table function — data never flows through Airflow.
   Airflow *coordinates*; it shouldn't be a data plane. Loads replace the
   day's partition, so re-running is safe (idempotency!).
3. **Transform** (`dbt_run`): staging views expose the latest snapshot;
   marts build `fct_orders` (grain: order item), `dim_product`,
   `dim_customer`, `mart_daily_revenue`.
4. **Test** (`dbt_test`): keys unique & not-null, statuses in the allowed
   set. Break something and watch the pipeline go red.

## Connect Metabase (one-time, ~2 minutes)

1. Open http://localhost:3000, create the local admin account.
2. Add database → ClickHouse → host `clickhouse`, port `8123`,
   username `shopstream`, password `shopstream`, database `analytics`.
3. Build a dashboard: revenue over time from `mart_daily_revenue`,
   orders by status from `fct_orders`. Save it as "ShopStream Ops".

## Things worth doing before moving on

1. **Re-run the DAG** (`make trigger-daily` again) and check ClickHouse row
   counts don't double — then read `load_table` to see why (partition
   replacement). Idempotency is *the* batch-pipeline skill.
2. **Compare the stores.** Run the same aggregate (say, revenue by
   category) in `make psql` and `make chsql`; look at `EXPLAIN` in both.
   Row store vs column store stops being abstract.
3. **Find the stale-status bug.** Right after a DAG run, `stg_orders` in
   ClickHouse matches Postgres. Ten minutes later it doesn't (statuses
   moved on). Batch snapshots are always stale — phase 2 (CDC) fixes this.
4. **Break a test.** Edit `_marts.yml` to disallow `'cancelled'` in
   `accepted_values`, re-trigger, and watch `dbt_test` fail the run.

## In production you would…

- split Airflow into separate scheduler / api-server / worker services
  (CeleryExecutor or KubernetesExecutor) with real authentication;
- extract incrementally (by `updated_at` watermark or CDC), not `SELECT *`;
- pin every image and Python dependency to exact versions;
- alert on DAG failures and data-test failures (phase 5).

**Next:** Phase 2 — Kafka, Avro clickstream events, and Debezium CDC.
