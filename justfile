#!/usr/bin/env just --justfile

# ═══════════════════════════════════════════════════════════════════════════
# HELP & DEFAULT
# ═══════════════════════════════════════════════════════════════════════════

default:
	@just --list

# ═══════════════════════════════════════════════════════════════════════════
# LOCAL DEVELOPMENT
# ═══════════════════════════════════════════════════════════════════════════

# Build and start the local dev stack
up:
	docker compose --file docker-compose-local.yml up -d --build
	@echo "🚀 Orchestrator running at http://localhost:${ORCHESTRATOR_PORT:-42069} (Docker)"
	@echo "🧪 Stub miner proxy running at http://localhost:${STUB_MINER_PORT:-8765}"
	@echo "   View logs: just devlogs or just orclogs"

# Start local dev stack without rebuilding
local:
	docker compose --file docker-compose-local.yml up -d
	@echo "🚀 Orchestrator running at http://localhost:${ORCHESTRATOR_PORT:-42069} (Docker)"
	@echo "🧪 Stub miner proxy running at http://localhost:${STUB_MINER_PORT:-8765}"
	@echo "   View logs: just devlogs or just orclogs"

# Rebuild and restart only the orchestrator-dev service
local-rebuild:
	docker compose --file docker-compose-local.yml up -d --build orchestrator-dev
	@echo "🚀 Orchestrator running at http://localhost:${ORCHESTRATOR_PORT:-42069} (Docker)"
	@echo "🧪 Stub miner proxy running at http://localhost:${STUB_MINER_PORT:-8765}"
	@echo "   View logs: just devlogs or just orclogs"

# Stop the local dev stack
local-down:
	docker compose --file docker-compose-local.yml down
	@echo "🛑 Local stack stopped"

# Tail logs for orchestrator and stub miner
devlogs tail='100':
	docker compose --file docker-compose-local.yml logs --tail={{tail}} -f orchestrator-dev stub-miner

# Tail logs for orchestrator only
orclogs tail='100':
	docker compose --file docker-compose-local.yml logs --tail={{tail}} -f orchestrator-dev

# Tail logs for the stub miner
stublogs tail='100':
	docker compose --file docker-compose-local.yml logs --tail={{tail}} -f stub-miner

# Restart the orchestrator-dev service
orchestrator-restart:
	docker compose --file docker-compose-local.yml restart orchestrator-dev
	@echo "🔁 Restarted orchestrator-dev"

# Restart the stub miner service
stub-restart:
	docker compose --file docker-compose-local.yml restart stub-miner
	@echo "🔁 Restarted stub-miner"

ss58-logs tail='100':
	docker compose --file docker-compose-local.yml logs --tail={{tail}} -f ss58

ss58-shell:
	docker compose --file docker-compose-local.yml exec ss58 bash

ss58-python script:
	docker compose --file docker-compose-local.yml exec ss58 python {{script}}

ss58-verify head='' public_key='' gateway='https://gateway.pinata.cloud/ipfs':
	@if [ -z "{{head}}" ] || [ -z "{{public_key}}" ]; then \
		echo "Usage: just ss58-verify head=<cid> public_key=<hex> [gateway=...]"; exit 1; \
	fi; \
	PYTHONPATH=ss58_service uv run --with-requirements ss58_service/ss58/requirements.ss58.txt \
		python -m ss58.verify_chain --head "{{head}}" --public-key-hex "{{public_key}}" --gateway "{{gateway}}"

# Drop into python-runner shell for ad-hoc scripts
python-shell:
	docker compose --file docker-compose-local.yml exec python-runner bash

# Run a Python script in the python-runner container
python-run script:
	docker compose --file docker-compose-local.yml exec python-runner python {{script}}

# ═══════════════════════════════════════════════════════════════════════════
# PRODUCTION
# ═══════════════════════════════════════════════════════════════════════════

# Start prod-equivalent stack
prod-up:
	DB_URL="${DATABASE_URL:-${PROD_DATABASE_URL:-}}"; \
	if [ -z "$DB_URL" ]; then \
		echo "DATABASE_URL or PROD_DATABASE_URL must be set for prod-up" >&2; \
		exit 1; \
	fi; \
	ENV_ARGS="--env-file .env"; \
	if [ -f .env.prod ]; then ENV_ARGS="$ENV_ARGS --env-file .env.prod"; fi; \
	DATABASE_URL="$DB_URL" just db-migrate; \
	docker compose $ENV_ARGS --file docker-compose-prod.yml up -d --build; \
	echo "🚀 Prod-equivalent stack running at http://localhost:${ORCHESTRATOR_PORT:-42069} (Docker)"; \
	echo "   View logs: just prod-logs"

# Stop prod-equivalent stack
prod-down:
	ENV_ARGS="--env-file .env"; \
	if [ -f .env.prod ]; then ENV_ARGS="$ENV_ARGS --env-file .env.prod"; fi; \
	docker compose $ENV_ARGS --file docker-compose-prod.yml down
	@echo "🛑 Prod-equivalent stack stopped"

# Tail logs for prod stack
prod-logs:
	ENV_ARGS="--env-file .env"; \
	if [ -f .env.prod ]; then ENV_ARGS="$ENV_ARGS --env-file .env.prod"; fi; \
	docker compose $ENV_ARGS --file docker-compose-prod.yml logs -f orchestrator redis

# Restart prod-equivalent services (keeps volumes/data)
prod-restart:
	ENV_ARGS="--env-file .env"; \
	if [ -f .env.prod ]; then ENV_ARGS="$ENV_ARGS --env-file .env.prod"; fi; \
	docker compose $ENV_ARGS --file docker-compose-prod.yml restart orchestrator forwarder redis
	@echo "🔁 Restarted prod stack services"

# Rebuild and restart prod-equivalent services
prod-rebuild:
	ENV_ARGS="--env-file .env"; \
	if [ -f .env.prod ]; then ENV_ARGS="$ENV_ARGS --env-file .env.prod"; fi; \
	docker compose $ENV_ARGS --file docker-compose-prod.yml up -d --build orchestrator forwarder redis
	@echo "🛠️  Rebuilt and restarted prod stack services"

# ═══════════════════════════════════════════════════════════════════════════
# WORKERS
# ═══════════════════════════════════════════════════════════════════════════

# Run metagraph refresh worker (env=local|prod)
run-metagraph env='local':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers metagraph

# Run score ETL worker
run-score-etl env='local' hotkey='':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	if [ -n "{{hotkey}}" ]; then \
		HOTKEY_FLAG="--hotkey {{hotkey}}"; \
	else \
		HOTKEY_FLAG=""; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers score $HOTKEY_FLAG

# Run audit seed worker
run-audit-seed env='local' job_type='' preview='false':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	if [ -n "{{job_type}}" ]; then \
		AUDIT_JOB_TYPE="--audit-job-type {{job_type}}"; \
	else \
		AUDIT_JOB_TYPE=""; \
	fi; \
	if [ "{{preview}}" = "true" ]; then \
		PREVIEW_FLAG="--audit-preview"; \
	else \
		PREVIEW_FLAG=""; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers audit-seed $AUDIT_JOB_TYPE $PREVIEW_FLAG

# Run audit check worker
run-audit-check env='local' apply='false':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	if [ "{{apply}}" = "true" ]; then \
		AUDIT_FLAGS="--audit-apply"; \
	else \
		AUDIT_FLAGS=""; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers audit-check $AUDIT_FLAGS

# Backwards-compatible alias for audit check
run-audit env='local' apply='false':
	@just run-audit-check env={{env}} apply={{apply}}

# Run audit broadcast worker
run-audit-broadcast env='local':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers audit-broadcast

# Replay latest audit job to all miners for matching job types
run-seed-requests env='local':
	ENV_RAW="{{env}}"; \
	if [ "${ENV_RAW#env=}" != "${ENV_RAW}" ]; then ENV_VALUE="${ENV_RAW#env=}"; else ENV_VALUE="${ENV_RAW}"; fi; \
	if [ "${ENV_VALUE}" = "prod" ]; then \
		COMPOSE_FILE="docker-compose-prod.yml"; \
		SERVICE="orchestrator"; \
	else \
		COMPOSE_FILE="docker-compose-local.yml"; \
		SERVICE="orchestrator-dev"; \
	fi; \
	docker compose --file "$COMPOSE_FILE" exec "$SERVICE" \
		python -m orchestrator.workers seed-requests

# ═══════════════════════════════════════════════════════════════════════════
# DATABASE MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

# Start standalone database
db-up:
	docker compose --file docker-compose-db.yml up -d

# Stop standalone database
db-down:
	docker compose --file docker-compose-db.yml down

# Build database schema migration image
db-schema-build:
	docker build -t just-orchestrator-db-schema:local database

# Run database migrations (requires DATABASE_URL env var)
db-migrate: db-schema-build
	@if [ -z "${DATABASE_URL}" ]; then echo "✗ DATABASE_URL must be set"; exit 1; fi
	docker run --rm --network host -e DATABASE_URL="${DATABASE_URL}" just-orchestrator-db-schema:local

# ═══════════════════════════════════════════════════════════════════════════
# TESTING
# ═══════════════════════════════════════════════════════════════════════════

# Start integration test database
integration-up:
	docker compose --file docker-compose-integration.yml up -d
	@echo "🗄️  Integration Postgres listening on postgresql://orchestrator:orchestrator@localhost:15432/orchestrator_test"

# Stop integration test database and clean volumes
integration-down:
	docker compose --file docker-compose-integration.yml down -v

# Run migrations on integration test database
integration-migrate:
	DATABASE_URL=${DATABASE_URL:-${INTEGRATION_DATABASE_URL:-postgresql://orchestrator:orchestrator@localhost:15432/orchestrator_test?sslmode=disable}} just db-migrate

# Run integration tests
integration-test args="": integration-migrate
	uv pip sync requirements.orchestrator.txt
	INTEGRATION_DATABASE_URL=${INTEGRATION_DATABASE_URL:-postgresql://orchestrator:orchestrator@localhost:15432/orchestrator_test?sslmode=disable} \
		uv run --with pytest --with-requirements requirements.orchestrator.txt \
			pytest -m "integration" orchestrator/tests/integration {{args}}

# Run functional tests against running orchestrator
functional-tests:
	API_BASE_URL="${API_BASE_URL:-http://localhost:${ORCHESTRATOR_PORT:-42069}}" \
		uv run --with pytest pytest functional_tests

# ═══════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

# Sync uv dependencies
uv-sync:
	uv pip sync requirements.orchestrator.txt

# Check import structure
imports-check:
	uv run python scripts/check_imports.py orchestrator

# Check environment configuration
check-env:
	@test -f .env && echo "✓ .env exists" || echo "✗ .env missing"
	@test -f .env.example && echo "✓ .env.example exists" || echo "✗ .env.example missing"

# Check Docker status
check-docker:
	@echo "Checking Docker containers..."
	@docker compose -f docker-compose-local.yml ps || echo "✗ Docker compose not available"

# Run all checks
check: check-env check-docker
	@echo "\n=== All checks completed ==="
