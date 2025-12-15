"""Command-line entrypoint for the SS58 signature service."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("SS58_HOST", "0.0.0.0")
    port = int(os.getenv("SS58_PORT", "8585"))
    reload = os.getenv("SS58_RELOAD", "false").lower() in {"1", "true", "yes", "on"}
    uvicorn.run("ss58.app:app", host=host, port=port, reload=reload, factory=False)


if __name__ == "__main__":  # pragma: no cover - thin CLI wrapper
    main()
