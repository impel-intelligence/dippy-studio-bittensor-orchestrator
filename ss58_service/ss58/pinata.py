"""Pinata client for pinning JSON payloads to IPFS."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests

LOGGER = logging.getLogger("ss58.pinata")


class PinataClient:
    """Lightweight wrapper over Pinata's JSON pinning API."""

    def __init__(
        self,
        *,
        base_url: str,
        jwt: str | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        timeout_seconds: int = 20,
    ) -> None:
        if not jwt and not (api_key and api_secret):
            raise ValueError("Pinata credentials are required (set SS58_PINATA_JWT or API key/secret)")
        self.jwt = jwt.strip() if jwt else None
        self.api_key = api_key.strip() if api_key else None
        self.api_secret = api_secret.strip() if api_secret else None
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def pin_json(self, payload: Dict[str, Any], name: Optional[str] = None) -> str:
        """Pin a JSON payload and return the resulting CID."""
        url = f"{self.base_url}/pinning/pinJSONToIPFS"
        body: Dict[str, Any] = {"pinataContent": payload}
        if name:
            body["pinataMetadata"] = {"name": name}

        headers = {}
        if self.jwt:
            headers["Authorization"] = f"Bearer {self.jwt}"
        else:
            headers["pinata_api_key"] = self.api_key or ""
            headers["pinata_secret_api_key"] = self.api_secret or ""
        resp = requests.post(url, json=body, headers=headers, timeout=self.timeout_seconds)
        try:
            resp.raise_for_status()
        except Exception:
            LOGGER.error("Pinata pinJSONToIPFS failed: %s", resp.text)
            raise
        data = resp.json()
        cid = data.get("IpfsHash") or data.get("Hash") or data.get("cid")
        if not cid:
            raise RuntimeError("Pinata response missing CID")
        return cid
