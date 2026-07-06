# Phase 5 — Observability: Prometheus, Grafana, Lag, Freshness, SLAs

**What you build:** eyes on the platform. Prometheus scrapes every
component (Kafka offsets & consumer lag, Airflow task outcomes via StatsD,
Postgres, ClickHouse, MinIO), Grafana ships with a provisioned ShopStream
dashboard, and four alert rules encode the pipeline's SLOs — including the
one that matters most and is easiest to miss: *data freshness*.

**What you learn:** the metrics that actually matter for data platforms
(lag, freshness, failure counts — not CPU), exporters vs native endpoints
vs StatsD (all three patterns are here), PromQL basics, alert design with
debounce windows, and observing an incident you cause on purpose.

---

## Run it

```bash
make up-all              # if the full platform isn't already running
make up-observability    # prometheus + grafana + exporters
```

| What | Where | Notes |
|---|---|---|
| Grafana | http://localhost:3001 | no login; "ShopStream Pipeline" dashboard is pre-provisioned |
| Prometheus | http://localhost:9090 | `/targets` = scrape health, `/alerts` = rule state |
| Exporters | :9308 kafka, :9102 airflow/statsd, :9187 postgres | raw `/metrics` text — worth reading once |

## How each component gets scraped (three patterns, all standard)

1. **Native endpoints** — ClickHouse (`:9363`, via a config.d file) and
   MinIO just serve `/metrics` themselves. Modern software mostly does.
2. **Exporters** — Kafka and Postgres predate Prometheus; a sidecar
   (`kafka-exporter`, `postgres-exporter`) translates their internal
   protocols into metrics.
3. **StatsD push** — Airflow fires UDP packets per event (task started,
   succeeded, failed…); `statsd-exporter` accumulates them into
   scrapeable counters. Push vs pull, in one stack.

## The four alerts (`observability/prometheus/alerts.yml`)

| Alert | Meaning | SLO it encodes |
|---|---|---|
| `ClickstreamStalled` | no events produced for 5 min | freshness: the stream is never quiet |
| `ConsumerLagHigh` | a consumer group >5000 behind | latency: streaming stays near-realtime |
| `DLQFlood` | poison messages >1/sec | quality: bad data stays exceptional |
| `AirflowTaskFailures` | any task failure in 30 min | reliability: batch runs green |

## Things worth doing before moving on

1. **Cause the lag alert — and learn a real gotcha on the way.** First
   surprise: the lag panel is empty even though two Spark jobs consume the
   topic. That's because `kafka-exporter` reads *committed consumer-group
   offsets*, and Spark Structured Streaming never commits to Kafka — it
   tracks offsets in its own checkpoints (monitoring Spark lag needs its
   metrics sink or checkpoint inspection; a classic production surprise).
   To see real group lag, create a committing consumer and abandon it:
   ```bash
   docker exec shopstream-kafka kafka-console-consumer \
     --bootstrap-server kafka:29092 --topic shopstream.events.v1 \
     --group lag-demo --from-beginning --max-messages 50
   ```
   The `lag-demo` group consumed 50 messages, committed, and left — now
   watch its lag climb forever on the panel, and `ConsumerLagHigh` fire
   after it passes 5000. Clean up with
   `kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group lag-demo`.
2. **Cause the freshness alert.** `docker stop shopstream-generator` —
   events/sec drops to zero and `ClickstreamStalled` goes pending → firing.
   This is the alert that catches "everything is up but no data is moving".
3. **Fail a task, see it surface.** Re-run the phase-4 constraint drill and
   watch the failure appear in the "Airflow task failures" stat panel and
   the `AirflowTaskFailures` alert.
4. **Read the raw metrics.** `curl -s localhost:9308/metrics | grep lag` —
   dashboards are just queries over this text.
5. **Write one PromQL query yourself.** In Prometheus: purchases-adjacent
   throughput per topic: `sum by (topic) (rate(kafka_topic_partition_current_offset[5m]))`.

## In production you would…

- add **Alertmanager** with routing to Slack/PagerDuty (the rules are
  ready; only delivery is missing);
- monitor Spark streaming batch duration & input rate (Spark's metrics
  system → Prometheus sink), and MinIO/ClickHouse disk headroom;
- track *data-level* freshness (max event_ts per table vs now) with a SQL
  exporter — offset-rate freshness is a proxy;
- keep dashboards in git exactly like this repo does (provisioning as
  code), never hand-edited in the UI only.

**Next:** Phase 6 — Kubernetes: k3d, Helm, operators; the platform
engineering side of data engineering.
