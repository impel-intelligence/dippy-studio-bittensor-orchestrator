from __future__ import annotations

import os

import uvicorn

from forwarder.app import create_app


app = create_app()


def main() -> None:
    port = int(os.getenv("FORWARDER_PORT", "9871"))
    reload = os.getenv("FORWARDER_RELOAD", "false").lower() == "true"
    uvicorn.run(
        "forwarder.server:app",
        host="0.0.0.0",
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    main()
