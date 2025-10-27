# just_orchestrator

This repository hosts the orchestration layer that coordinates miners, validators, and auxiliary services. The development workflow runs entirely in Docker and uses FastAPI for the HTTP interface.

## Quick Start
- Copy `.env.example` to `.env` and fill in the required secrets.
- Start the dev stack: `just up`. To skip rebuilding images use `just local`.
- Confirm services are healthy:
  - Orchestrator API: `http://localhost:42169/health`
  - Jobrelay API: `http://localhost:8181/health`
- Tail logs with `just orclogs` or `docker compose -f docker-compose-local.yml logs -f jobrelay`.

## Tests
- Unit tests (containerized): `docker compose -f docker-compose-local.yml run --rm orchestrator-dev pytest orchestrator/tests -q`.
- Functional tests (against the running stack): see `AGENTS.md` for the full workflow.
- Integration tests that rely on a dedicated Postgres instance are documented in `INTERNAL.md`.

## Database Migrations
- Run `DATABASE_URL="postgres://..." just db-migrate` to build the Atlas image and apply migrations.
- Additional Atlas commands and automation guidance live in `database/instructions.md`.

## Additional Documentation
- `AGENTS.md` captures the end-to-end development playbook used across agents.
- `INTERNAL.md` aggregates integration-test steps, score persistence notes, and other internal references.
- `database/instructions.md` describes the Atlas-backed migration pipeline.
