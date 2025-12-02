# just_orchestrator

This repository hosts the orchestration layer that coordinates miners, validators, and auxiliary services. The development workflow runs entirely in Docker and uses FastAPI for the HTTP interface.

## Quick Start
- Copy `.env.example` to `.env` and fill in the required secrets.
- Start the dev stack: `just up`. To skip rebuilding images use `just local`.
- Confirm services are healthy:
  - Orchestrator API: `http://localhost:8338/health`
  - Jobrelay API: `http://localhost:8181/health`
  - Stub miner API: `http://localhost:8765/`
- Tail logs with `just devlogs` (or `just orclogs` for orchestrator-only).

## Tests
- Unit tests (containerized): `docker compose -f docker-compose-local.yml run --rm orchestrator-dev pytest orchestrator/tests -q`.
- Functional tests (against the running stack): see `AGENTS.md` for the full workflow.
- Integration tests that rely on a dedicated Postgres instance are documented in `INTERNAL.md`.

## Database Migrations
- Run `DATABASE_URL="postgres://..." just db-migrate` to build the Atlas image and apply migrations.
- Additional Atlas commands and automation guidance live in `database/instructions.md`.
