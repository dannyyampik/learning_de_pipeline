# Phase 2 — Streaming: Kafka, Avro Clickstream, Debezium CDC

**What you build:** the streaming backbone. The generator becomes "v2" and
starts emitting Avro-serialized clickstream events (real browsing sessions
walking the shopping funnel) to Kafka, while Debezium tails Postgres's
write-ahead log and turns every INSERT/UPDATE/DELETE into a CDC event —
no changes to the app required.

**What you learn:** topics/partitions/keys and advertised listeners,
schemas-on-the-wire with Schema Registry + Avro, consumer groups and lag,
and change data capture — logical replication, snapshots, tombstones, and
why CDC beats daily snapshot extracts.

---

## Run it

```bash
make up-streaming    # core + kafka + schema-registry + connect + kafka-ui
make ps              # kafka-init & connect-init are one-shot jobs (Exited 0 = good)
make connect-status  # Debezium connector should be RUNNING
make consume-events  # deserialized clickstream, live (Ctrl-C to stop)
make consume-cdc     # raw CDC feed for orders (Ctrl-C to stop)
```

| What | Where | Notes |
|---|---|---|
| Kafka UI | http://localhost:8085 | topics, messages, schemas, consumers, connect |
| Kafka (from host) | `localhost:9092` | from containers: `kafka:29092` |
| Schema Registry | http://localhost:8081 | try `/subjects` and `/subjects/shopstream.events.v1-value/versions/1` |
| Kafka Connect | http://localhost:8083 | Debezium REST API |

Phase 1's batch stack is not required for this phase (`make up-all` runs both).

## What's flowing

**Clickstream** (`shopstream.events.v1`, 3 partitions, keyed by session_id):
browsing sessions step through `page_view / product_view / search →
add_to_cart → begin_checkout → purchase`. A purchase creates a *real order
in Postgres* and stamps the order_id into the event — stream and database
can be reconciled later. ~35% of sessions browse anonymously (null
customer_id) and sign up at checkout. ~2% of events arrive with event
timestamps minutes in the past (late data — phase 3 watermark fodder), and
~0.5% are deliberately malformed non-Avro bytes (future DLQ lesson).

**CDC** (`cdc.shopstream.public.*`, from Debezium): every row change in
the six OLTP tables, with `before`/`after` images and `op` (c/u/d). On
first start Debezium snapshots existing rows (`op: r`), then streams the
WAL from there.

## Things worth doing before moving on

1. **Watch one session.** In Kafka UI, pick a `session_id` from a message
   and filter the topic by that key — see the funnel play out in order,
   and notice all its events sit in the same partition. That's what the
   key is for.
2. **Catch an order mutating.** `make consume-cdc` and wait ~2 minutes:
   you'll see the same order_id appear repeatedly with `op: u` as its
   status advances — every intermediate state phase 1's snapshots missed.
3. **Find a tombstone.** When a GDPR delete fires (~every 2h, or crank
   `CUSTOMER_DELETES_PER_HOUR`), `cdc.shopstream.public.customers` gets a
   `op: d` event with `before` populated, followed by a null-value
   tombstone message.
4. **Break the schema (safely).** Try registering an incompatible schema
   version via the registry API and watch it get rejected — that's the
   contract doing its job. `curl -X POST http://localhost:8081/compatibility/subjects/shopstream.events.v1-value/versions/latest ...`
5. **Peek at Postgres internals.** `make psql`:
   `SELECT * FROM pg_replication_slots;` — that's Debezium holding its
   place in the WAL. This is also why an idle Debezium can bloat a real
   database's disk (retained WAL).

## Design notes / trade-offs

- **Clickstream uses Avro + Schema Registry; CDC uses plain JSON.** You
  should experience both encodings. Avro-encoding the CDC topics too (via
  Apicurio or Confluent converters) is a good extra-credit exercise.
- **Single broker, RF=1**: fine locally; real clusters run 3+ brokers,
  RF=3, `min.insync.replicas=2` — the runbook pattern "in production…"
  applies to everything here.

## In production you would…

- run 3+ brokers with rack awareness, TLS + SASL auth, and quotas;
- monitor consumer lag (phase 5) and replication slot lag on Postgres;
- manage connectors declaratively (CI applies the JSON) and alert on task
  failures — a silently stopped connector is a data outage;
- think hard about topic retention & compaction per topic (default here:
  broker defaults, 7 days).

**Next:** Phase 3 — Spark Structured Streaming reads these topics into
Iceberg bronze tables on MinIO, and a realtime KPI job feeds ClickHouse.
