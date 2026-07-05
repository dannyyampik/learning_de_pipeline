# ShopStream learning platform — entry points.
# Each phase gets its own compose file; `up-<phase>` starts only what that
# phase needs, so you never run more infrastructure than the current lesson.

COMPOSE_CORE := docker compose -f docker/compose.core.yml

.PHONY: help up-core down ps logs logs-generator psql demo nuke

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up-core: ## Start phase 0: Postgres + generator + MinIO
	$(COMPOSE_CORE) up -d --build

down: ## Stop everything (keeps data volumes)
	$(COMPOSE_CORE) down

ps: ## Show container status
	$(COMPOSE_CORE) ps

logs: ## Tail all logs
	$(COMPOSE_CORE) logs -f --tail=50

logs-generator: ## Tail just the traffic generator
	$(COMPOSE_CORE) logs -f --tail=50 generator

psql: ## Open a psql shell into the OLTP database
	$(COMPOSE_CORE) exec postgres psql -U shopstream -d shopstream

demo: ## Show what the app has been up to (row counts, statuses, revenue)
	@$(COMPOSE_CORE) exec -T postgres psql -U shopstream -d shopstream -f - < db/demo.sql

nuke: ## Stop everything AND delete data volumes (fresh start)
	$(COMPOSE_CORE) down -v
