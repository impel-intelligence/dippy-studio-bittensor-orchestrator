#!/usr/bin/env just --justfile

default:
	@just --list

setup: _ensure-venv
	source .venv/bin/activate
	echo "🔧 Setting up project..."
	uv pip install -e .
	if [ -f "requirements.miner.txt" ]; then
		echo "📦 Installing miner dependencies..."
		uv pip install -r requirements.miner.txt
	fi
	if [ -f "requirements.orchestrator.txt" ]; then
		echo "📦 Installing orchestrator dependencies..."
		uv pip install -r requirements.orchestrator.txt
	fi
	if [ -f "requirements.validator.txt" ]; then
		echo "📦 Installing validator dependencies..."
		uv pip install -r requirements.validator.txt
	fi
	echo "📦 Installing test dependencies..."
	uv pip install pytest pytest-cov pytest-mock
	echo "✅ Setup complete!"

up:
	docker compose --file docker-compose-dev.yml up -d --build

down:
	docker compose -f docker-compose-dev.yml down

devlogs:
	docker compose --file docker-compose-dev.yml logs -f orchestrator-dev

dev:
	docker compose --file docker-compose-dev.yml up -d
	@echo "🚀 Orchestrator running at http://localhost:42069 (Docker)"
	@echo "   View logs: just devlogs"
dev-rebuild:
	docker compose --file docker-compose-dev.yml up -d --build orchestrator-dev
	@echo "🚀 Orchestrator running at http://localhost:42069 (Docker)"
	@echo "   View logs: just devlogs"

orchestrator-restart:
	docker compose --file docker-compose-dev.yml restart orchestrator-dev
	@echo "🔁 Restarted orchestrator-dev"

functional-tests: _ensure-venv
	source .venv/bin/activate
	API_BASE_URL="${API_BASE_URL:-http://localhost:42069}" pytest functional_tests

unit-test: _ensure-venv
	source .venv/bin/activate
	echo "🧪 Running unit tests..."
	test_failed=0
	
	if [ -d "orchestrator/tests" ] && [ "$(ls -A orchestrator/tests/test_*.py 2>/dev/null)" ]; then
		echo "📦 Running orchestrator unit tests..."
		python -m pytest orchestrator/tests/ -v || test_failed=1
	fi
	
	if [ -d "miner/tests" ] && [ "$(ls -A miner/tests/test_*.py 2>/dev/null)" ]; then
		echo "📦 Running miner unit tests..."
		python -m pytest miner/tests/ -v || test_failed=1
	fi
	
	if [ -d "validator/tests" ] && [ "$(ls -A validator/tests/test_*.py 2>/dev/null)" ]; then
		echo "📦 Running validator unit tests..."
		python -m pytest validator/tests/ -v || test_failed=1
	fi
	
	if [ -d "epistula/tests" ] && [ "$(ls -A epistula/tests/test_*.py 2>/dev/null)" ]; then
		echo "📦 Running epistula unit tests..."
		python -m pytest epistula/tests/ -v || test_failed=1
	fi
	
	exit $test_failed

unit-test-coverage: _ensure-venv
	source .venv/bin/activate
	echo "🧪 Running unit tests with coverage..."
	python -m pytest \
		orchestrator/tests/ \
		miner/tests/ \
		validator/tests/ \
		epistula/tests/ \
		-v \
		--cov=orchestrator \
		--cov=miner \
		--cov=validator \
		--cov=epistula \
		--cov-report=term-missing \
		--cov-report=html \
		--ignore=functional_tests/

unit-test-specific test: _ensure-venv
	source .venv/bin/activate
	echo "🧪 Running specific test: {{test}}"
	python -m pytest {{test}} -vv -s

imports-check:
	python3 scripts/check_imports.py orchestrator

_ensure-venv:
	if [ ! -d ".venv" ]; then
		echo "✗ Virtual environment not found at .venv"
		echo "Please create it with: uv venv"
		exit 1
	fi
	if [ ! -f ".venv/bin/activate" ]; then
		echo "✗ Virtual environment appears corrupted (no activate script)"
		exit 1
	fi
	if ! uv pip show sn11 > /dev/null 2>&1; then
		echo "📦 Installing package in editable mode for absolute imports..."
		uv pip install -e .
	fi

check-env:
	@test -f .env && echo "✓ .env exists" || echo "✗ .env missing"
	@test -f .env.example && echo "✓ .env.example exists" || echo "✗ .env.example missing"

check-docker:
	@echo "Checking Docker containers..."
	@docker compose ps || echo "✗ Docker compose not available"

check-venv:
	@test -d .venv && echo "✓ Virtual environment exists" || echo "✗ Virtual environment missing"
	@test -d .venv && echo "✓ Python version: $(. .venv/bin/activate && python --version)" || true

check-deps:
	@test -d .venv && echo "✓ Packages: $(. .venv/bin/activate && uv pip list | wc -l)" || echo "✗ Cannot check - venv missing"

check: check-env check-docker check-venv check-deps
	@echo "\n=== All checks completed ==="

clean:
	docker compose down
	docker system prune -f

validator-test coldkey="default" hotkey="default":
	if [ ! -d ".vali" ]; then
		echo "✗ Validator virtual environment not found at .vali"
		echo "Please create it first with your validator dependencies"
		exit 1
	fi
	if [ ! -f ".vali/bin/activate" ]; then
		echo "✗ Validator virtual environment appears corrupted (no activate script)"
		exit 1
	fi
	
	source .vali/bin/activate
	echo "✓ Activated validator virtual environment"
	echo "Running validator with:"
	echo "  - Network: test"
	echo "  - Mode: immediate"
	echo "  - Coldkey: {{coldkey}}"
	echo "  - Hotkey: {{hotkey}}"
	echo ""
	
	python validator/validator.py \
		--network test \
		--netuid 231 \
		--immediate \
		--wallet.name {{coldkey}} \
		--wallet.hotkey {{hotkey}} \
		--logging.debug

miner-dev:
	docker compose --file docker-compose-miner.yml up -d --build miner-dev
	@echo "🚀 Miner running at http://localhost:${MINER_PORT:-8001} (Docker)"
	@echo "   Stop with: docker compose -f docker-compose-miner.yml down"
