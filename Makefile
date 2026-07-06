# ShopStream learning platform — entry points.
# Each phase gets its own compose file; `up-<phase>` starts only what that
# phase needs, so you never run more infrastructure than the current lesson.

CORE   := -f docker/compose.core.yml
OLAP   := -f docker/compose.olap.yml
ORCH   := -f docker/compose.orchestration.yml
STREAM := -f docker/compose.streaming.yml
LAKE   := -f docker/compose.lakehouse.yml
OBS    := -f docker/compose.observability.yml

COMPOSE_CORE   := docker compose $(CORE)
COMPOSE_BATCH  := docker compose $(CORE) $(OLAP) $(ORCH)
COMPOSE_STREAM := docker compose $(CORE) $(STREAM)
COMPOSE_LAKE   := docker compose $(CORE) $(STREAM) $(OLAP) $(LAKE)
# The superset of everything defined so far (used by down/ps/logs/nuke)
COMPOSE_ALL    := docker compose $(CORE) $(OLAP) $(ORCH) $(STREAM) $(LAKE) $(OBS)

# Compose files share one project; don't warn about services from other files
export COMPOSE_IGNORE_ORPHANS := 1

.PHONY: help up-core up-batch up-streaming up-lakehouse up-observability up-all down ps logs \
        logs-generator psql chsql demo demo-olap demo-rt trigger-daily \
        consume-events consume-cdc connect-status spark-sql nuke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up-core: ## Phase 0: Postgres + generator + MinIO
	$(COMPOSE_CORE) up -d --build

up-batch: ## Phase 1: core + ClickHouse + Airflow + Metabase
	$(COMPOSE_BATCH) up -d --build

up-streaming: ## Phase 2: core + Kafka + Schema Registry + Debezium + Kafka UI
	$(COMPOSE_STREAM) up -d --build

up-lakehouse: ## Phase 3: streaming stack + ClickHouse + Iceberg + Spark jobs
	$(COMPOSE_LAKE) up -d --build

up-observability: ## Phase 5: Prometheus + Grafana + exporters (add to any stack)
	docker compose $(CORE) $(OBS) up -d

up-all: ## Everything defined so far
	$(COMPOSE_ALL) up -d --build

down: ## Stop everything (keeps data volumes)
	$(COMPOSE_ALL) down

ps: ## Show container status
	$(COMPOSE_ALL) ps

logs: ## Tail all logs
	$(COMPOSE_ALL) logs -f --tail=50

logs-generator: ## Tail just the traffic generator
	$(COMPOSE_ALL) logs -f --tail=50 generator

psql: ## psql shell into the OLTP database
	$(COMPOSE_ALL) exec postgres psql -U shopstream -d shopstream

chsql: ## clickhouse-client shell into the warehouse
	$(COMPOSE_ALL) exec clickhouse clickhouse-client --user shopstream --password shopstream

demo: ## OLTP pulse check (row counts, statuses, revenue)
	@$(COMPOSE_ALL) exec -T postgres psql -U shopstream -d shopstream -f - < db/demo.sql

demo-olap: ## Warehouse pulse check (raw loads + marts)
	@$(COMPOSE_ALL) exec -T clickhouse clickhouse-client --user shopstream --password shopstream \
		--multiquery --format PrettyCompact < db/demo_olap.sql

trigger-daily: ## Trigger the daily_batch DAG right now
	$(COMPOSE_ALL) exec airflow airflow dags trigger daily_batch

trigger-silver: ## Trigger the hourly_silver DAG right now
	$(COMPOSE_ALL) exec airflow airflow dags trigger hourly_silver

trigger-maintenance: ## Trigger the weekly_maintenance DAG right now
	$(COMPOSE_ALL) exec airflow airflow dags trigger weekly_maintenance

demo-silver: ## Silver layer pulse check (row counts per table)
	@$(COMPOSE_ALL) exec -T spark-connect /opt/spark/bin/spark-sql -e \
		"SHOW TABLES IN lake.silver; \
		 SELECT 'events' t, count(*) FROM lake.silver.events UNION ALL \
		 SELECT 'orders_current', count(*) FROM lake.silver.orders_current UNION ALL \
		 SELECT 'orders_history', count(*) FROM lake.silver.orders_history UNION ALL \
		 SELECT 'customers_current', count(*) FROM lake.silver.customers_current UNION ALL \
		 SELECT 'products_current', count(*) FROM lake.silver.products_current;" 2>/dev/null

consume-events: ## Tail the Avro clickstream topic (deserialized), Ctrl-C to stop
	$(COMPOSE_ALL) exec schema-registry kafka-avro-console-consumer \
		--bootstrap-server kafka:29092 --topic shopstream.events.v1 \
		--property schema.registry.url=http://localhost:8081

consume-cdc: ## Tail the orders CDC topic (JSON), Ctrl-C to stop
	$(COMPOSE_ALL) exec kafka kafka-console-consumer \
		--bootstrap-server kafka:29092 --topic cdc.shopstream.public.orders

connect-status: ## Show Debezium connector status
	@curl -s http://localhost:8083/connectors/shopstream-cdc/status | python3 -m json.tool

spark-sql: ## Interactive Spark SQL shell on the lakehouse (try: SELECT * FROM lake.bronze.events LIMIT 5;)
	$(COMPOSE_ALL) exec spark-bronze /opt/spark/bin/spark-sql

demo-rt: ## Realtime KPIs from ClickHouse (last 10 minutes)
	@$(COMPOSE_ALL) exec -T clickhouse clickhouse-client --user shopstream --password shopstream \
		--query "SELECT window_start, sum(events) events, max(sessions) sessions, sum(purchases) purchases, round(sum(revenue),2) revenue FROM rt.kpis_minute FINAL WHERE window_start > now() - INTERVAL 10 MINUTE GROUP BY window_start ORDER BY window_start DESC FORMAT PrettyCompact"

nuke: ## Stop everything AND delete data volumes (fresh start)
	$(COMPOSE_ALL) down -v

# ---------------------------------------------------------------------------
# Phase 6: Kubernetes (requires k3d, kubectl, helm — see runbook)
# ---------------------------------------------------------------------------
.PHONY: k8s-up k8s-core k8s-kafka k8s-status k8s-down

k8s-up: ## Create the k3d cluster (1 server + 2 agents, in Docker)
	k3d cluster create --config k8s/cluster/k3d-config.yaml

k8s-core: ## Deploy Postgres + MinIO + generator to the cluster
	docker build -t shopstream/generator:local generator
	k3d image import -c shopstream shopstream/generator:local
	kubectl kustomize --load-restrictor LoadRestrictionsNone k8s/core | kubectl apply -f -

k8s-kafka: ## Install the Strimzi operator and declare the Kafka cluster + topics
	helm repo add strimzi https://strimzi.io/charts/ 2>/dev/null || true
	helm upgrade --install strimzi strimzi/strimzi-kafka-operator \
		--namespace shopstream --create-namespace \
		--set watchNamespaces={shopstream}
	kubectl apply -f k8s/kafka/kafka.yaml

k8s-status: ## Everything in the shopstream namespace
	kubectl -n shopstream get pods,svc,pvc,jobs

k8s-down: ## Delete the whole cluster (all state gone)
	k3d cluster delete shopstream
