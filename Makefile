.PHONY: help up deploy verify status logs down reset smoke

help:
	@echo "Usage:"
	@echo "  make up       Build and start the stack, then deploy the Flink jobs"
	@echo "  make deploy   Submit any Flink job that isn't running (idempotent)"
	@echo "  make verify   Row counts, live insert/update/delete + latency, gold vs. Postgres"
	@echo "  make status   Flink jobs, replication slot, ClickHouse freshness"
	@echo "  make logs     Tail all service logs"
	@echo "  make down     Stop the stack, keep data"
	@echo "  make reset    Stop the stack and wipe all data"
	@echo "  make smoke    up + verify (what CI runs)"

up:
	cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build --wait
	./scripts/deploy.sh
	@echo ""
	@echo "Stack ready. Run 'make verify'."
	@echo "  Flink UI:        http://localhost:8089"
	@echo "  ClickHouse HTTP: http://localhost:8125"
	@echo "  Source Postgres: localhost:5435"

deploy:
	./scripts/deploy.sh

verify:
	./scripts/verify.sh

status:
	./scripts/status.sh

logs:
	docker compose logs -f

down:
	docker compose down

reset:
	docker compose down -v
	@echo "All volumes wiped. Run 'make up' to start fresh."

smoke: up verify
