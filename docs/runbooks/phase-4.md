# Phase 4 — Quality & Reliability: Silver Layer, Gates, Maintenance

**What you build:** the layer that turns raw streams into *trustworthy*
tables, plus the safety rails: an hourly bronze→silver job (dedupe + typed
CDC flattening) that ends in a **quality gate that fails the pipeline**,
and a weekly maintenance DAG (compaction, snapshot expiry, DLQ archive).
Airflow drives Spark remotely over **Spark Connect** — no JVM in Airflow.

**What you learn:** the medallion contract (what silver promises that
bronze doesn't), deduplication and why at-least-once delivery makes it
mandatory, unwrapping Debezium envelopes into history/current tables,
quality gates as first-class pipeline citizens, cross-domain
reconciliation, and the operational chores (small files, metadata growth)
every lakehouse owner inherits.

---

## Run it

```bash
make up-all            # everything: core + streaming + olap + lakehouse + airflow
make trigger-silver    # or unpause hourly_silver in the UI at :00 past each hour
make demo-silver       # silver row counts once the run is green
make trigger-maintenance
```

The two new DAGs (`hourly_silver`, `weekly_maintenance`) appear in the
Airflow UI (http://localhost:8080). They need the lakehouse stack running —
if you're only doing phase 1, leave them paused.

## What the hourly run does

1. **`silver_events`** — rebuilds `lake.silver.events` deduplicated by
   `event_id` (first arrival wins). Bronze keeps the duplicates on purpose:
   you can always measure how dirty the input was.
2. **`silver_cdc[orders|customers|products]`** — unwraps the Debezium JSON
   envelope into `<t>_history` (every change, typed, including deletes) and
   `<t>_current` (latest state per key; a delete-as-latest means the row is
   gone — GDPR erasures actually disappear here).
3. **`quality_gate`** — SQL assertions: no duplicate/null keys, purchases
   carry order_ids, no negative totals, and a **reconciliation check**:
   every purchase event older than 10 minutes must have a matching order in
   the CDC feed. Any violation raises and the run goes red.

`CREATE OR REPLACE` rebuilds are used for teachability: atomic (Iceberg
swaps snapshots), idempotent, no watermark state. The production-scale
alternative — incremental `MERGE INTO` keyed on the last processed
offset/timestamp — is the natural exercise once row counts grow.

## Things worth doing before moving on

1. **Watch the gate catch something.** The OLTP schema's CHECK constraint
   blocks negative totals — so play the classic incident where the app team
   drops a constraint in a migration and your gate becomes the last line of
   defense. In `make psql`:
   ```sql
   ALTER TABLE orders DROP CONSTRAINT orders_total_amount_check;
   UPDATE orders SET total_amount = -1
   WHERE order_id = (SELECT max(order_id) FROM orders);
   ```
   then `make trigger-silver`: CDC picks up the update, silver rebuilds,
   and `quality_gate` fails naming the offending order_id. Fix the row,
   re-add the constraint, re-run — green. That loop is the whole point.
2. **Compare bronze vs silver.** `SELECT count(*) FROM lake.bronze.events`
   vs `lake.silver.events` — the difference is duplicates (idempotent
   producer keeps it small; crank generator restarts to grow it).
3. **Watch an order's whole life.**
   `SELECT status, changed_at, op FROM lake.silver.orders_history WHERE order_id = <id> ORDER BY changed_at;`
   — pending → paid → shipped → delivered, every state CDC captured. This
   is the table phase 1's daily snapshots could never give you.
4. **Count files before/after compaction.**
   `SELECT count(*) FROM lake.bronze.events.files;` then
   `make trigger-maintenance`, then count again. That's the small-files
   problem being solved.
5. **Query the DLQ archive.**
   `SELECT CAST(value AS STRING), kafka_ts FROM lake.bronze.dlq_archive LIMIT 5;`
   — the poison messages, now just rows you can inspect with SQL.

## Design note: where's Great Expectations?

The design listed GX as the quality tool. The *pattern* — declarative
checks between layers that stop the pipeline — is implemented here in
plain SQL, which keeps it fully transparent while you learn the concept.
Swapping the `quality_gate` task for a GX checkpoint (or Soda scan) is a
great extra-credit exercise once the pattern feels obvious; the DAG shape
doesn't change.

## In production you would…

- run silver incrementally (MERGE on watermarks) instead of full rebuilds;
- alert on gate failures (phase 5) and track check results over time;
- add schema-drift detection on `cdc_raw` (Debezium happily forwards
  upstream DDL changes — a new OLTP column appears in the JSON silently);
- run maintenance per table with tuned targets, not a one-size loop.

**Next:** Phase 5 — observability: Prometheus + Grafana, consumer lag,
pipeline SLAs, freshness alerts.
