# Phase 3 — Lakehouse: Spark Structured Streaming + Iceberg, Realtime KPIs

**What you build:** the lakehouse. Spark Structured Streaming continuously
lands the clickstream and CDC feeds into **Apache Iceberg** tables on MinIO
(the bronze layer), quarantines poison messages into a DLQ topic, and a
second Spark job maintains a live 1-minute KPI table in ClickHouse — the
"speed layer" next to phase 1's batch layer.

**What you learn:** Iceberg (catalogs, hidden partitioning, snapshots &
time travel), Structured Streaming (checkpoints, triggers, exactly-once
sinks), the Confluent Avro wire format, poison-pill handling & DLQs,
event time vs processing time, watermarks and late data, and why
"Lambda architecture" means maintaining two code paths.

---

## Run it

```bash
make up-lakehouse    # streaming stack + ClickHouse + Iceberg REST + 2 Spark jobs
make ps              # wait for spark-bronze & spark-kpis to settle (~1 min)
make spark-sql       # then: SELECT count(*) FROM lake.bronze.events;
make demo-rt         # live KPIs, refresh a few times
```

| What | Where | Notes |
|---|---|---|
| Iceberg REST catalog | http://localhost:8181 | `/v1/config`, `/v1/namespaces/bronze/tables` |
| Bronze data itself | http://localhost:9001 | MinIO console → `lake/lakehouse/…` — Parquet + JSON metadata |
| Spark UIs | http://localhost:4040 (in-container) | per-job streaming metrics |
| Live KPIs | `rt.kpis_minute` in ClickHouse | `make demo-rt` or Metabase on it |

## The moving parts

- **`jobs/streaming/bronze_ingest.py`** — one Spark app, three queries:
  clickstream → `lake.bronze.events` (Avro decoded PERMISSIVE-ly; the
  ~0.5% malformed messages can't crash the stream), bad messages →
  `shopstream.events.dlq` byte-for-byte, and all `cdc.*` topics →
  `lake.bronze.cdc_raw` untouched. Bronze keeps everything; cleaning is
  silver's job (phase 4).
- **`jobs/streaming/realtime_kpis.py`** — its own consumer group; 1-minute
  event-time windows with a 2-minute watermark, written to a ClickHouse
  `ReplacingMergeTree` in update mode: late data re-emits a window and
  ClickHouse keeps the freshest row (query with `FINAL`).
- **`spark/`** — the image bakes in exactly the jars a Spark+Kafka+Iceberg
  runtime needs (read the Dockerfile; that list *is* the lesson) and a
  `spark-defaults.conf` that wires the `lake` catalog to the Iceberg REST
  catalog, which stores files on MinIO.

## Things worth doing before moving on

1. **Time travel.** In `make spark-sql`:
   `SELECT * FROM lake.bronze.events.snapshots;` then
   `SELECT count(*) FROM lake.bronze.events VERSION AS OF <snapshot_id>;`
   Every streaming micro-batch is a snapshot you can query or roll back to.
2. **Look at the files.** MinIO console → `lake/lakehouse/bronze/events/` —
   `data/` has small Parquet files (one per micro-batch; that's the
   "small files problem", fixed by compaction in phase 4), `metadata/`
   has the JSON/Avro manifests that make it a *table* and not just files.
3. **Inspect the DLQ.** `docker compose ... exec kafka kafka-console-consumer
   --bootstrap-server kafka:29092 --topic shopstream.events.dlq --from-beginning`
   — there are the poison pills, quarantined, not lost, not fatal.
4. **See the watermark work.** Compare a minute's `events` in
   `rt.kpis_minute` against bronze for the same minute — bronze eventually
   has slightly more (events later than the 2-min watermark were dropped
   from the live aggregate but still landed in bronze). Speed layer =
   approximately right now; batch layer = exactly right later.
5. **Kill and resume.** `docker restart shopstream-spark-bronze`, then
   check no events were lost or duplicated in bronze (checkpoint +
   Iceberg's idempotent commits = exactly-once sink).

## Scope note

The design's bronze → silver batch job and dbt-over-silver move to
**phase 4 (quality & reliability)** — they're about cleaning (dedupe by
event_id, flatten CDC envelopes, quarantine review) which is exactly that
phase's theme. This keeps each phase one concept deep.

## In production you would…

- run Spark on Kubernetes or YARN with dynamic executors — not
  `local[*]` drivers in single containers;
- checkpoint to object storage, not a local volume;
- schedule Iceberg maintenance from day one: compaction, snapshot expiry,
  orphan-file cleanup (phase 4);
- monitor consumer lag and micro-batch duration (phase 5) — a streaming
  job that's silently 4 hours behind is a subtle outage.

**Next:** Phase 4 — data quality: bronze→silver with dedupe & typed CDC,
Great Expectations gates, DLQ replay, Iceberg maintenance.
