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
| 1 | Batch ELT: Airflow → lake → ClickHouse → dbt → Metabase | ✅ | [runbook](docs/runbooks/phase-1.md) |
| 2 | Streaming: Kafka, Avro events, Debezium CDC | ✅ | [runbook](docs/runbooks/phase-2.md) |
| 3 | Lakehouse: Spark + Iceberg bronze, realtime KPIs | ✅ | [runbook](docs/runbooks/phase-3.md) |
| 4 | Quality & reliability: silver layer, gates, maintenance | ✅ | [runbook](docs/runbooks/phase-4.md) |
| 5 | Observability: Prometheus, Grafana, lag & freshness alerts | ✅ | [runbook](docs/runbooks/phase-5.md) |
| 6 | Kubernetes: k3d, manifests, Strimzi operator | ✅ | [runbook](docs/runbooks/phase-6.md) |

## Quickstart (phase 0)

Requirements: `docker` (with compose v2) and `make`.

```bash
make up-core   # start Postgres + the ShopStream traffic generator + MinIO
make demo      # see what the app has been doing
make help      # everything else
```
