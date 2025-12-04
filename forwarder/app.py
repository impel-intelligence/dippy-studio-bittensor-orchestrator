from __future__ import annotations

import random
import os
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, Field

LISTEN_AUTH_HEADER = "X-Service-Auth-Secret"
DEFAULT_LISTEN_SECRET = "orchestrator-listen-secret"
DEFAULT_JOB_TYPE = "base-h100_pcie"
DEFAULT_ORCHESTRATOR_URL = "http://orchestrator:42169"


class Settings(BaseModel):
    orchestrator_base_url: str = Field(default=DEFAULT_ORCHESTRATOR_URL)
    listen_auth_secret: str = Field(default=DEFAULT_LISTEN_SECRET, min_length=1, max_length=255)
    job_type: str = Field(default=DEFAULT_JOB_TYPE, min_length=1, max_length=255)
    request_timeout_seconds: float = Field(default=10.0, gt=0)

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            orchestrator_base_url=_string_env(
                "FORWARDER_ORCHESTRATOR_BASE_URL",
                fallback=os.getenv("ORCHESTRATOR_BASE_URL", DEFAULT_ORCHESTRATOR_URL),
            ),
            listen_auth_secret=_string_env(
                "FORWARDER_LISTEN_AUTH_SECRET",
                fallback=os.getenv("LISTEN_AUTH_SECRET", DEFAULT_LISTEN_SECRET),
            ),
            job_type=_string_env("FORWARDER_JOB_TYPE", fallback=DEFAULT_JOB_TYPE),
            request_timeout_seconds=_float_env("FORWARDER_TIMEOUT_SECONDS", fallback=10.0),
        )


class ForwardRequest(BaseModel):
    prompt: str = Field(..., min_length=1, max_length=255)
    webhook_url: str = Field(..., min_length=1, max_length=255)
    route_to_auditor: bool | None = None


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    app = FastAPI(title="Forwarder", version="0.1.0")

    app.state.settings = settings
    app.state.http_client = None

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.http_client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        client = getattr(app.state, "http_client", None)
        if client:
            await client.aclose()

    @app.post("/listen/remote", status_code=status.HTTP_202_ACCEPTED)
    async def forward_job(request: ForwardRequest) -> Response:
        route_to_auditor = request.route_to_auditor
        if route_to_auditor is None:
            # Randomly route ~5% of requests to the audit miner.
            route_to_auditor = random.random() < 0.05

        payload = {
            "job_type": settings.job_type,
            "payload": {"prompt": request.prompt},
            "webhook_url": request.webhook_url,
            "route_to_auditor": route_to_auditor,
        }
        target_url = f"{settings.orchestrator_base_url.rstrip('/')}/listen/remote"
        headers = {LISTEN_AUTH_HEADER: settings.listen_auth_secret}

        client: httpx.AsyncClient | None = app.state.http_client
        created_client = False
        if client is None:
            # Fallback for cases where startup has not run yet.
            client = httpx.AsyncClient(timeout=settings.request_timeout_seconds)
            created_client = True

        try:
            resp = await client.post(target_url, json=payload, headers=headers)
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to reach orchestrator: {exc}",
            ) from exc
        finally:
            if created_client:
                await client.aclose()

        return Response(
            content=resp.content,
            status_code=resp.status_code,
            media_type=resp.headers.get("content-type"),
        )

    return app


def _string_env(key: str, fallback: str) -> str:
    value = os.getenv(key)
    if value is None:
        return fallback
    return value


def _float_env(key: str, fallback: float) -> float:
    raw_value = os.getenv(key)
    if raw_value is None:
        return fallback
    try:
        return float(raw_value)
    except ValueError:
        return fallback
