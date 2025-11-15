from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import ConnectionPool


class PostgresClient:
    """Lightweight wrapper around a psycopg connection pool."""

    def __init__(
        self,
        dsn: str,
        *,
        min_connections: int = 1,
        max_connections: int = 10,
        kwargs: Optional[dict[str, object]] = None,
    ) -> None:
        if not dsn:
            raise ValueError("PostgresClient requires a PostgreSQL DSN")

        pool_kwargs = {"autocommit": True}
        if kwargs:
            pool_kwargs.update(kwargs)

        self._dsn = dsn
        self._pool = ConnectionPool(
            conninfo=self._dsn,
            min_size=max(1, min_connections),
            max_size=max(1, max_connections),
            kwargs=pool_kwargs,
            open=True,
        )

    @property
    def dsn(self) -> str:
        return self._dsn

    @property
    def safe_dsn(self) -> str:
        try:
            params = conninfo_to_dict(self._dsn)
        except Exception:
            return self._dsn

        if "password" in params and params["password"] is not None:
            params["password"] = "***"

        try:
            return make_conninfo(**params)
        except Exception:
            filtered = [f"{key}={value}" for key, value in params.items() if value is not None]
            return " ".join(filtered) if filtered else self._dsn

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        with self._pool.connection() as conn:
            yield conn

    @contextmanager
    def cursor(self) -> Iterator[psycopg.Cursor]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                yield cur

    def close(self) -> None:
        try:
            self._pool.close()
            self._pool.wait()
        except Exception:
            # Allow double close in teardown paths without raising.
            pass


__all__ = ["PostgresClient"]
