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
	@echo "🚀 Orchestrator running at http://localhost:42169 (Docker)"
	@echo "   View logs: just devlogs or just orclogs"

# Start local dev stack without rebuilding
local:
	docker compose --file docker-compose-local.yml up -d
	@echo "🚀 Orchestrator running at http://localhost:42169 (Docker)"
	@echo "   View logs: just devlogs or just orclogs"

# Rebuild and restart only the orchestrator-dev service
local-rebuild:
	docker compose --file docker-compose-local.yml up -d --build orchestrator-dev
	@echo "🚀 Orchestrator running at http://localhost:42169 (Docker)"
	@echo "   View logs: just devlogs or just orclogs"

# Stop the local dev stack
local-down:
	docker compose --file docker-compose-local.yml down
	@echo "🛑 Local stack stopped"

# Tail logs for both orchestrator and jobrelay
devlogs:
	docker compose --file docker-compose-local.yml logs -f orchestrator-dev jobrelay

# Tail logs for orchestrator only
orclogs tail='':
	@if [ -n "{{tail}}" ]; then \
		TAIL_FLAG="--tail={{tail}}"; \
	else \
		TAIL_FLAG=""; \
	fi; \
	docker compose --file docker-compose-local.yml logs ${TAIL_FLAG} -f orchestrator-dev

# Restart the orchestrator-dev service
orchestrator-restart:
	docker compose --file docker-compose-local.yml restart orchestrator-dev
	@echo "🔁 Restarted orchestrator-dev"

# ═══════════════════════════════════════════════════════════════════════════
# PRODUCTION
# ═══════════════════════════════════════════════════════════════════════════

# Start prod-equivalent stack
prod-up:
	docker compose --file docker-compose-prod.yml up -d --build
	@echo "🚀 Prod-equivalent stack running at http://localhost:42169 (Docker)"
	@echo "   View logs: just prod-logs"

# Stop prod-equivalent stack
prod-down:
	docker compose --file docker-compose-prod.yml down
	@echo "🛑 Prod-equivalent stack stopped"

# Tail logs for prod stack
prod-logs:
	docker compose --file docker-compose-prod.yml logs -f orchestrator-prod jobrelay-prod

# ═══════════════════════════════════════════════════════════════════════════
# WORKERS
# ═══════════════════════════════════════════════════════════════════════════

# Run metagraph refresh worker once
run-metagraph-once:
	docker compose --file docker-compose-local.yml exec orchestrator-dev \
		python -m orchestrator.workers metagraph

# Run score ETL worker once
run-score-etl-once hotkey='':
	@if [ -n "{{hotkey}}" ]; then \
		HOTKEY_FLAG="--hotkey {{hotkey}}"; \
	else \
		HOTKEY_FLAG=""; \
	fi; \
	docker compose --file docker-compose-local.yml exec orchestrator-dev \
		python -m orchestrator.workers score $HOTKEY_FLAG

# Run audit seed worker once
run-audit-seed-once:
	docker compose --file docker-compose-local.yml exec orchestrator-dev \
		python -m orchestrator.workers audit-seed

# Run audit check worker once
run-audit-check-once apply='false':
	@if [ "{{apply}}" = "true" ]; then \
		AUDIT_FLAGS="--audit-apply"; \
	else \
		AUDIT_FLAGS=""; \
	fi; \
	docker compose --file docker-compose-local.yml exec orchestrator-dev \
		python -m orchestrator.workers audit-check $AUDIT_FLAGS

# Backwards-compatible alias for audit check
run-audit-once apply='false':
	@just run-audit-check-once apply={{apply}}

# Run all workers once (metagraph + score ETL)
run-workers-once:
	docker compose --file docker-compose-local.yml exec orchestrator-dev \
		python -m orchestrator.workers all

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
	API_BASE_URL="${API_BASE_URL:-http://localhost:42169}" \
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

# ═══════════════════════════════════════════════════════════════════════════
# JOBRELAY UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

# Execute the JobRelay CLI helper inside Docker (pass args="completed ...")
jobrelay-cli args="--help":
	docker compose --file docker-compose-local.yml run --rm jobrelay \
		python -m jobrelay.cli {{args}}
