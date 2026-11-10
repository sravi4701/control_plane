.DEFAULT_GOAL := help
COMPOSE := docker compose
TEST := $(COMPOSE) --profile test run --rm --build test

.PHONY: help up down clean logs ps test lint fmt race queues worker-fail worker-chaos worker-default

help: ## Show available commands
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-16s %s\n", $$1, $$2}'

up: ## Build and boot everything (Postgres, RabbitMQ, control plane, worker)
	$(COMPOSE) up --build --detach --wait
	@echo "API docs:    http://localhost:$${API_PORT:-8000}/docs"
	@echo "RabbitMQ UI: http://localhost:$${RABBITMQ_MGMT_PORT:-15672} (control_plane / control_plane)"

down: ## Stop everything (data is kept)
	$(COMPOSE) --profile test down

clean: ## Stop everything and delete all data volumes
	$(COMPOSE) --profile test down --volumes --remove-orphans

logs: ## Follow control-plane and worker logs
	$(COMPOSE) logs -f control-plane worker

ps: ## Show service status
	$(COMPOSE) ps

test: ## Run all quality gates + the full test suite (lint, format, types, security, tests)
	$(TEST)

lint: ## Lint, format check, type check and security scan only (no tests)
	$(TEST) sh -c 'ruff check . && ruff format --check . && mypy && bandit -q -c pyproject.toml -r app worker'

fmt: ## Format and auto-fix code in your working tree
	$(COMPOSE) --profile test run --rm --build -v "$(CURDIR):/app" test sh -c 'ruff format . && ruff check --fix .'

race: ## Hammer the running stack (make up first) with concurrent requests; checks invariants
	$(TEST) python scripts/race.py --base-url http://control-plane:8000

queues: ## Show queue depths (including the dead-letter queues)
	$(COMPOSE) exec rabbitmq rabbitmqctl list_queues --quiet name messages consumers

worker-fail: ## Restart the worker so every task fails
	WORKER_FAIL_RATE=1 $(COMPOSE) up --detach --force-recreate worker

worker-chaos: ## Restart the worker: every update sent twice, terminal before in_progress
	WORKER_DUPLICATE_RATE=1 WORKER_REORDER=true $(COMPOSE) up --detach --force-recreate worker

worker-default: ## Restart the worker with default (happy-path) settings
	$(COMPOSE) up --detach --force-recreate worker
