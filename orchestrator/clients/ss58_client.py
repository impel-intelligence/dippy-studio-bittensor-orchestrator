"""Async client for interacting with the SS58 append-only service."""

from __future__ import annotations

import logging
from typing import Iterable, Sequence

import httpx

logger = logging.getLogger(__name__)


class SS58Client:
    """Lightweight HTTP client for the SS58 ban log."""

    def __init__(self, base_url: str | None, *, timeout_seconds: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/") if base_url else None
        try:
            timeout = float(timeout_seconds)
        except (TypeError, ValueError):
            timeout = 5.0
        self._timeout = 5.0 if timeout <= 0.0 else timeout

    @property
    def enabled(self) -> bool:
        return bool(self._base_url)

    async def append_addresses(self, addresses: Sequence[str]) -> bool:
        """Append one or more addresses to the SS58 log."""
        normalized = self._normalize_addresses(addresses)
        if not normalized:
            return False
        if not self._base_url:
            logger.debug("ss58_client.append.skipped disabled")
            return False

        url = f"{self._base_url}/add"
        payload = {"addresses": sorted(normalized)}
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
            return True
        except Exception as exc:  # pragma: no cover - defensive network guard
            logger.warning(
                "ss58_client.append_failed url=%s count=%s error=%s",
                url,
                len(normalized),
                exc,
            )
            return False

    async def list_addresses(self) -> set[str]:
        """Return the full set of addresses registered in the SS58 log."""
        if not self._base_url:
            return set()

        url = f"{self._base_url}/addresses"
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
        except Exception as exc:  # pragma: no cover - defensive network guard
            logger.warning("ss58_client.fetch_failed url=%s error=%s", url, exc)
            return set()

        try:
            payload = response.json()
        except Exception:  # pragma: no cover - non-JSON payload
            logger.warning(
                "ss58_client.fetch_invalid_json url=%s status=%s body_preview=%s",
                url,
                response.status_code,
                response.text[:200],
            )
            return set()

        if isinstance(payload, dict):
            addresses_raw = payload.get("addresses", []) or []
        elif isinstance(payload, list):
            addresses_raw = payload
        else:
            addresses_raw = []

        return self._normalize_addresses(addresses_raw)

    @staticmethod
    def _normalize_addresses(addresses: Iterable[str]) -> set[str]:
        normalized: set[str] = set()
        for addr in addresses:
            candidate = str(addr).strip()
            if candidate:
                normalized.add(candidate)
        return normalized


__all__ = ["SS58Client"]
