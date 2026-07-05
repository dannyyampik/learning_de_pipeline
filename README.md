# learning_de_pipeline

A hands-on learning project that builds a complete data engineering platform
around **ShopStream**, a fictional e-commerce app — using the tools real data
teams use today (Postgres, Kafka, Debezium, Spark, Iceberg, dbt, Airflow,
ClickHouse, Metabase), all self-hosted on Docker for ~$0.

**Start here:** [docs/DESIGN.md](docs/DESIGN.md) — architecture, schemas,
tool rationale, and the full learning roadmap.

## Phases

| Phase | Topic | Status | Runbook |
|---|---|---|---|
| 0 | Foundations: OLTP app + traffic generator + MinIO | ✅ | [runbook](docs/runbooks/phase-0.md) |
| 1 | Batch ELT: Airflow → lake → ClickHouse → dbt → Metabase | — | |
| 2 | Streaming: Kafka, Avro events, Debezium CDC | — | |
| 3 | Lakehouse: Spark + Iceberg medallion, realtime KPIs | — | |
| 4 | Data quality & reliability | — | |
| 5 | Observability | — | |
| 6 | Kubernetes | — | |

## Quickstart (phase 0)

Requirements: `docker` (with compose v2) and `make`.

```bash
make up-core   # start Postgres + the ShopStream traffic generator + MinIO
make demo      # see what the app has been doing
make help      # everything else
```
