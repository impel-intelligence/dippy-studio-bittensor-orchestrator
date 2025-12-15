"""Async client for interacting with the job relay service."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)


JOBRELAY_AUTH_HEADER = "X-Service-Auth-Secret"


class BaseJobRelayClient:

    async def create_job(self, job_id: UUID, payload: Dict[str, Any]) -> None:  # pragma: no cover - noop
        logger.debug("jobrelay.create.noop job_id=%s", job_id)

    async def update_job(self, job_id: UUID, updates: Dict[str, Any]) -> None:  # pragma: no cover - noop
        logger.debug("jobrelay.update.noop job_id=%s updates=%s", job_id, list(updates.keys()))

    async def fetch_job(self, job_id: UUID) -> Optional[Dict[str, Any]]:  # pragma: no cover - noop
        logger.debug("jobrelay.fetch.noop job_id=%s", job_id)
        return None

    async def list_jobs(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        source: str | None = None,
        limit: int | None = None,
    ) -> list[Dict[str, Any]]:  # pragma: no cover - noop
        logger.debug(
            "jobrelay.list.noop start=%s end=%s source=%s limit=%s",
            start.isoformat() if start else None,
            end.isoformat() if end else None,
            source,
            limit,
        )
        return []

    async def list_recent_jobs(self, *, limit: int) -> list[Dict[str, Any]]:  # pragma: no cover - noop
        logger.debug("jobrelay.list_recent.noop limit=%s", limit)
        return []

    async def list_jobs_for_hotkey(
        self,
        hotkey: str,
        since: datetime | None = None,
    ) -> list[Dict[str, Any]]:  # pragma: no cover - noop
        if since:
            logger.debug(
                "jobrelay.list_hotkey.noop hotkey=%s since=%s",
                hotkey,
                since.isoformat(),
            )
        else:
            logger.debug("jobrelay.list_hotkey.noop hotkey=%s", hotkey)
        return []

    async def verify_connection(self) -> None:  # pragma: no cover - noop
        logger.debug("jobrelay.verify.noop")


@dataclass
class JobRelaySettings:
    base_url: str
    auth_token: Optional[str] = None
    timeout_seconds: float = 5.0


class JobRelayHttpClient(BaseJobRelayClient):

    def __init__(self, settings: JobRelaySettings) -> None:
        self._base_url = settings.base_url.rstrip("/")
        self._auth_header = JOBRELAY_AUTH_HEADER
        self._auth_token = settings.auth_token
        self._timeout = settings.timeout_seconds

    async def create_job(self, job_id: UUID, payload: Dict[str, Any]) -> None:
        await self._request("POST", f"/jobs/{job_id}", json=payload, params=None)

    async def update_job(self, job_id: UUID, updates: Dict[str, Any]) -> None:
        if not updates:
            return
        await self._request("PATCH", f"/jobs/{job_id}", json=updates, params=None)

    async def fetch_job(self, job_id: UUID) -> Optional[Dict[str, Any]]:
        response = await self._request(
            "GET",
            f"/jobs/{job_id}",
            json=None,
            params=None,
            allow_404=True,
        )
        if response is None:
            return None
        return response

    async def list_jobs(
        self,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        source: str | None = None,
        limit: int | None = None,
    ) -> list[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if start is not None:
            params["start"] = self._normalize_timestamp(start)
        if end is not None:
            params["end"] = self._normalize_timestamp(end)
        if source:
            params["source"] = source
        if limit is not None:
            try:
                params["limit"] = max(0, int(limit))
            except (TypeError, ValueError):
                params["limit"] = 0
        payload = await self._request("GET", "/jobs", json=None, params=params or None)
        if isinstance(payload, dict):
            jobs = payload.get("jobs", [])
            if isinstance(jobs, list):
                return jobs
        return []

    async def list_recent_jobs(self, *, limit: int) -> list[Dict[str, Any]]:
        return await self.list_jobs(limit=limit)

    async def list_jobs_for_hotkey(
        self,
        hotkey: str,
        since: datetime | None = None,
    ) -> list[Dict[str, Any]]:
        params: Dict[str, Any] | None = None
        if since is not None:
            params = {"since": self._normalize_timestamp(since)}
        payload = await self._request(
            "GET",
            f"/hotkeys/{hotkey}/jobs",
            json=None,
            params=params,
        )
        if isinstance(payload, dict):
            jobs = payload.get("jobs", [])
            if isinstance(jobs, list):
                return jobs
        return []

    async def verify_connection(self) -> None:
        await self._request("GET", "/health", json=None, params=None)

    async def _request(
        self,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]],
        *,
        params: Optional[Dict[str, Any]],
        allow_404: bool = False,
    ) -> Any:
        url = f"{self._base_url}{path}"
        headers: Dict[str, str] = {}
        if self._auth_token:
            headers[self._auth_header] = self._auth_token

        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.request(method, url, json=json, params=params, headers=headers)
            if allow_404 and response.status_code == 404:
                return None
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:  # pragma: no cover - network error reporting
                logger.warning(
                    "jobrelay.request.failed method=%s url=%s status=%s body=%s",
                    method,
                    url,
                    exc.response.status_code if exc.response else "n/a",
                    exc.response.text if exc.response else "",
                )
                raise
        if response.content:
            try:
                return response.json()
            except ValueError:  # pragma: no cover - response not json
                return None
        return None

    @staticmethod
    def _normalize_timestamp(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")


__all__ = [
    "BaseJobRelayClient",
    "JobRelayHttpClient",
    "JobRelaySettings",
]
