# learning_de_pipeline

A hands-on learning project that builds a complete data engineering platform
around **ShopStream**, a fictional e-commerce app — using the tools real data
teams use today (Postgres, Kafka, Debezium, Spark, Iceberg, dbt, Airflow,
ClickHouse, Metabase), all self-hosted on Docker for ~$0.

**Start here:** [docs/DESIGN.md](docs/DESIGN.md) — architecture, schemas,
tool rationale, and the full learning roadmap.

## How to use this repo

1. Read the design doc once (skim is fine — you'll come back to it).
2. Do the phases **in order**; each has a runbook with a "Run it" section,
   an explanation of what's happening, and hands-on exercises. The
   exercises are the actual learning — don't skip them.
3. Each phase has its own `make up-<phase>` target that starts only what
   that phase needs, so you never debug more infrastructure than the
   current lesson requires. `make down` between phases is fine — data
   lives in Docker volumes and survives; `make nuke` wipes everything for
   a fresh start.
4. When something is unclear, read the source — every compose file, DAG,
   job, and model is commented with *why*, not just *what*.

## Phases

| Phase | Topic | Status | Runbook |
|---|---|---|---|
| 0 | Foundations: OLTP app + traffic generator + MinIO | ✅ | [runbook](docs/runbooks/phase-0.md) |
| 1 | Batch ELT: Airflow → lake → ClickHouse → dbt → Metabase | ✅ | [runbook](docs/runbooks/phase-1.md) |
| 2 | Streaming: Kafka, Avro events, Debezium CDC | ✅ | [runbook](docs/runbooks/phase-2.md) |
| 3 | Lakehouse: Spark + Iceberg bronze, realtime KPIs | ✅ | [runbook](docs/runbooks/phase-3.md) |
| 4 | Quality & reliability: silver layer, gates, maintenance | ✅ | [runbook](docs/runbooks/phase-4.md) |
| 5 | Observability: Prometheus, Grafana, lag & freshness alerts | ✅ | [runbook](docs/runbooks/phase-5.md) |
| 6 | Kubernetes: k3d, manifests, Strimzi operator | ✅ | [runbook](docs/runbooks/phase-6.md) |

## Quickstart (phase 0)

Requirements: `docker` (with compose v2) and `make`. RAM guide: phases 0–2
run comfortably in ~8 GB of Docker memory; phases 3–5 (Spark + everything)
want ~12–16 GB. Phase 6 additionally needs `k3d`, `kubectl`, and `helm`.

```bash
make up-core   # start Postgres + the ShopStream traffic generator + MinIO
make demo      # see what the app has been doing
make help      # everything else
```
