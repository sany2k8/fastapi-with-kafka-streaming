.DEFAULT_GOAL := help
COMPOSE := docker compose

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Start the whole stack (Kafka, Postgres, API, 3 consumers)
	$(COMPOSE) up -d --build
	@echo "API      http://localhost:8700/docs"
	@echo "Inspect  http://localhost:8700/kafka/inspect"

ui: ## Also start Kafka UI on http://localhost:8092
	$(COMPOSE) --profile tools up -d kafka-ui

down: ## Stop everything (keeps data)
	$(COMPOSE) down

clean: ## Stop everything and delete Kafka log + Postgres data
	$(COMPOSE) down -v

logs: ## Tail all logs
	$(COMPOSE) logs -f

detector-logs: ## Tail only the fraud detector (shows partition/offset per event)
	$(COMPOSE) logs -f fraud-detector

scale: ## Run 3 fraud detectors and watch the rebalance
	$(COMPOSE) up -d --scale fraud-detector=3

unscale: ## Back to a single fraud detector
	$(COMPOSE) up -d --scale fraud-detector=1

demo: ## Full guided walkthrough: normal payment, then a fraud burst
	./scripts/demo.sh

burst: ## Fire 20 payments for one user in 30s (triggers the HIGH path)
	./scripts/burst.sh

inspect: ## Print topics, partitions, offsets and consumer lag
	@curl -s http://localhost:8700/kafka/inspect | python3 scripts/_show.py kafka

replay: ## Rewind audit-logger to offset 0 and reprocess every fraud event
	./scripts/replay.sh

test: ## Run the test suite (integration tests need `make up` first)
	cd backend && .venv/bin/python -m pytest tests -q

frontend: ## Run the dashboard on http://localhost:5193
	cd frontend && npm install && npm run dev

.PHONY: help up ui down clean logs detector-logs scale unscale demo burst inspect replay test frontend
