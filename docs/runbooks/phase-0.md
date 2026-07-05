# Phase 0 — Foundations: the App, its Database, and the (empty) Lake

**What you build:** the fictional ShopStream app — a PostgreSQL OLTP database
plus a Python traffic generator that behaves like the app's backend — and a
MinIO object store that will become the data lake in later phases.

**What you learn:** Docker Compose fundamentals (networks, volumes,
healthchecks, init scripts), OLTP data modeling, and how realistic
operational data actually behaves (it *mutates*, which is the root cause of
half of data engineering).

---

## Run it

```bash
make up-core        # build + start postgres, generator, minio
make ps             # everything should be healthy (minio-init exits 0 — it's a one-shot job)
make logs-generator # watch the app "do things"
make demo           # row counts, order statuses, revenue — run it twice, compare
make psql           # poke around yourself
make down           # stop (keeps data)   |   make nuke  # stop + wipe data
```

Endpoints:

| What | Where | Credentials |
|---|---|---|
| Postgres | `localhost:5432`, db `shopstream` | `shopstream` / `shopstream` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | same |

## What's happening

The generator ticks every second and rolls dice against configured rates
(Poisson-distributed, so traffic looks organic). Per default config
(`docker/compose.core.yml`) it: creates ~12 orders and ~2 customers per
minute, walks orders through `pending → paid → shipped → delivered` (or
`cancelled`), changes a product price ~6×/hour, and hard-deletes a customer
(GDPR erasure) ~every 2 hours.

## Things worth doing before moving on

1. **Watch a single order's lifecycle.** `make psql`, pick a recent order,
   re-query it over a few minutes and watch `status` and `updated_at` change.
   *This* is why daily snapshot extracts are lossy — the thing phase 2 (CDC) fixes.
2. **Find the learning traps.** The last section of `make demo` counts
   price-changed products and orphaned orders. Both will bite us later —
   deliberately.
3. **Turn the knobs.** Edit the generator's environment in
   `docker/compose.core.yml` (e.g. `ORDERS_PER_MINUTE: "120"`) and
   `make up-core` again — compose only recreates the generator.
4. **Look at the WAL setting.** `SHOW wal_level;` in psql → `logical`.
   That's the Postgres feature CDC will ride on in phase 2.

## In production you would…

- never hardcode credentials in compose files (secrets manager / env injection);
- run Postgres with backups, replicas, and connection pooling (PgBouncer);
- not give the analytics team direct access to the OLTP database at all —
  which is precisely why the rest of this project exists.

**Next:** [Phase 1](phase-1.md) — Airflow-orchestrated batch ELT:
Postgres → MinIO (Parquet) → ClickHouse → dbt → Metabase.
