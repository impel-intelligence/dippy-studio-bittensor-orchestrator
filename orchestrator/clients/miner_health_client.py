from __future__ import annotations

import json
import logging
from typing import Dict, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from orchestrator.common.epistula_client import EpistulaClient


class MinerHealthClient:
    """Network-facing helpers for miner health checks."""

    def __init__(
        self,
        *,
        epistula_client: EpistulaClient | None = None,
    ) -> None:
        self._epistula_client = epistula_client
        self._logger = logging.getLogger(__name__)

    def check_network_health(self, address: str, hotkey: str) -> bool:
        """Check if a miner's network endpoint is reachable."""
        logger = self._logger
        address_candidate = address
        try:
            parsed = urlparse(address_candidate)
            if not parsed.scheme:
                address_candidate = f"https://{address_candidate}/check/{hotkey}"
                parsed = urlparse(address_candidate)

            if not parsed.netloc:
                return False

            req = Request(address_candidate, method="GET")
            with urlopen(req, timeout=10) as response:  # noqa: S310 - controlled URL
                return response.status == 200
        except (URLError, HTTPError, Exception) as error:
            logger.debug("miner_health.network_unreachable address=%s error=%s", address_candidate, error)
            return False

    def fetch_capacity(
        self,
        network_address: str,
        hotkey: str,
    ) -> Tuple[Optional[Dict[str, bool]], bool]:
        """Fetch miner capacity metadata and report whether parsing failed."""
        if not self._epistula_client:
            return None, False

        log = self._logger
        parsed = urlparse(network_address)
        if not parsed.scheme:
            parsed = urlparse(f"https://{network_address}/check/{hotkey}")

        capacity_url = f"{parsed.scheme}://{parsed.netloc}/capacity"
        try:
            status_code, response_text = self._epistula_client.get_signed_request_sync(
                url=capacity_url,
                miner_hotkey=hotkey,
                timeout=10,
            )
        except (URLError, HTTPError, Exception) as error:
            log.warning("miner_health.capacity_fetch_failed url=%s error=%s", capacity_url, error)
            return None, False

        if status_code != 200:
            return None, False

        try:
            return self._normalize_capacity_payload(response_text), False
        except (json.JSONDecodeError, ValueError) as error:
            log.error(
                "miner_health.capacity_parse_failed url=%s hotkey=%s error=%s",
                capacity_url,
                hotkey,
                error,
            )
            return {}, True

    def _normalize_capacity_payload(self, response_text: str) -> Dict[str, bool]:
        data = json.loads(response_text)
        if not isinstance(data, dict):
            raise ValueError("Capacity payload must be an object")

        capabilities: Dict[str, bool] = {}
        for raw_key, raw_value in data.items():
            if not isinstance(raw_key, str):
                raise ValueError("Capability keys must be strings")
            normalized_key = raw_key.strip()
            if not normalized_key:
                continue
            if not isinstance(raw_value, bool):
                raise ValueError("Capability values must be booleans")
            capabilities[normalized_key] = raw_value

        return capabilities


__all__ = ["MinerHealthClient"]
