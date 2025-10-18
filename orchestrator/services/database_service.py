from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Optional

import pysqlite3 as sqlite3  # Use bundled modern SQLite with JSON1 support


class DatabaseService:

    def __init__(self, db_path: Optional[str | Path] = None) -> None:
        if db_path is None:
            tmp_dir = Path("/tmp")
            tmp_dir.mkdir(parents=True, exist_ok=True)
            db_path = tmp_dir / f"orchestrator_state-{os.getpid()}-{uuid.uuid4().hex}.db"
        else:
            db_path = Path(db_path)

        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: Optional[sqlite3.Connection] = self._create_connection()

    @property
    def path(self) -> Path:
        return self._db_path

    def get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection

    def create_connection(self) -> sqlite3.Connection:
        return self._create_connection()

    def close(self) -> None:
        if self._connection is None:
            return
        try:
            self._connection.close()
        except Exception:
            pass
        finally:
            self._connection = None

    def _create_connection(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self._db_path))
        self._configure_connection(connection)
        return connection

    @staticmethod
    def _configure_connection(connection: sqlite3.Connection) -> None:
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
