"""Command-line entrypoint for the job relay service."""

from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.getenv("JOBRELAY_HOST", "0.0.0.0")
    port = int(os.getenv("JOBRELAY_PORT", "8181"))
    reload = os.getenv("JOBRELAY_RELOAD", "false").lower() in {"1", "true", "yes"}
    uvicorn.run("jobrelay.app:app", host=host, port=port, reload=reload, factory=False)


if __name__ == "__main__":  # pragma: no cover - simple cli wrapper
    main()
