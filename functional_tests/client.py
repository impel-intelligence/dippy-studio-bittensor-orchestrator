from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx


class ApiClient:
    """Simple wrapper around :pypi:`httpx` for talking to the subnet service endpoints.

    The base URL can be provided explicitly or picked up from the
    ``API_BASE_URL`` environment variable. All helper methods strip leading
    or trailing slashes so callers can be flexible with the path they pass
    in.
    """

    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url: str = (
            base_url or os.getenv("API_BASE_URL", "http://localhost:8000")
        ).rstrip("/")
        self.client: httpx.Client = httpx.Client(base_url=self.base_url)

    def get(self, path: str, **kwargs: Any) -> httpx.Response:  # type: ignore[arg-type]
        return self.client.get(path, **kwargs)

    def post(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:  # type: ignore[arg-type]
        return self.client.post(path, json=json, **kwargs)

    def put(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:  # type: ignore[arg-type]
        return self.client.put(path, json=json, **kwargs)

    def patch(self, path: str, json: Optional[Dict[str, Any]] = None, **kwargs: Any) -> httpx.Response:  # type: ignore[arg-type]
        return self.client.patch(path, json=json, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> httpx.Response:  # type: ignore[arg-type]
        return self.client.delete(path, **kwargs)

    def health(self) -> httpx.Response:
        """Hit the canonical `/health` endpoint and return the response."""

        return self.get("/health")

