"""Storage helpers for maintaining the head pointer."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Optional

from .config import HeadState

LOGGER = logging.getLogger("ss58.store")


class HeadStore:
    """Persist the current head CID to disk."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> Optional[HeadState]:
        if not self.path.exists():
            return None
        try:
            data = json.loads(self.path.read_text())
            return HeadState(**data)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.error("Failed to read head file %s: %s", self.path, exc)
            return None

    def write(self, cid: str) -> HeadState:
        state = HeadState(cid=cid, updated_at=int(time.time()))
        self.path.write_text(state.model_dump_json())
        return state
