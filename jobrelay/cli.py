"""Command-line helpers for JobRelay operations."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from dateutil import parser as date_parser

from .config import get_settings
from .models import JobStatus
from .postgres_manager import JobRelayPostgresManager


def _parse_timestamp(value: str) -> datetime:
    """Parse ISO-8601 timestamps and normalize to UTC."""

    try:
        parsed = date_parser.isoparse(value)
    except (ValueError, TypeError) as exc:  # pragma: no cover - defensive against argparse quirks
        raise argparse.ArgumentTypeError(f"invalid timestamp '{value}': {exc}") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _rowdicts(columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[dict[str, Any]]:
    return [dict(zip(columns, row)) for row in rows]


def _emit_json(rows: list[dict[str, Any]]) -> None:
    print(json.dumps(rows, default=_json_default, indent=2))


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(value)


def _emit_table(rows: list[dict[str, Any]], include_payload: bool) -> None:
    if not rows:
        print("No jobs matched the requested filters.")
        return
    headers = ["job_id", "completed_at", "status", "miner_hotkey", "execution_duration_ms"]
    if include_payload:
        headers.extend(["payload", "response_payload"])
    widths: dict[str, int] = {header: len(header) for header in headers}
    for row in rows:
        for header in headers:
            value = row.get(header)
            rendered = _render_cell(value)
            widths[header] = max(widths[header], len(rendered))

    def _print_row(row_values: Sequence[str]) -> None:
        padded = [value.ljust(widths[header]) for value, header in zip(row_values, headers, strict=False)]
        print("  ".join(padded))

    _print_row(headers)
    print("  ".join("-" * widths[header] for header in headers))
    for row in rows:
        columns = [_render_cell(row.get(header)) for header in headers]
        _print_row(columns)


def _render_cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=_json_default, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _build_query(args: argparse.Namespace) -> tuple[str, list[Any]]:
    columns = [
        "CAST(job_id AS VARCHAR) AS job_id",
        "job_type",
        "miner_hotkey",
        "status",
        "completed_at",
        "execution_duration_ms",
        "result_image_url",
        "audit_status",
        "verification_status",
    ]
    if args.show_payload:
        columns.extend(["payload", "response_payload"])
    clauses = ["completed_at IS NOT NULL", "status <> 'pending'"]
    params: list[Any] = []
    if args.job_type:
        clauses.append("job_type = ?")
        params.append(args.job_type)
    if args.status:
        placeholders = ", ".join("?" for _ in args.status)
        clauses.append(f"status IN ({placeholders})")
        params.extend(args.status)
    if args.hotkey:
        clauses.append("miner_hotkey = ?")
        params.append(args.hotkey)
    if args.since:
        clauses.append("completed_at >= ?")
        params.append(args.since)
    where_clause = " AND ".join(clauses)
    query = f"""
        SELECT {", ".join(columns)}
        FROM inference_jobs
        WHERE {where_clause}
        ORDER BY completed_at DESC, job_id
    """
    if args.limit:
        query += " LIMIT ?"
        params.append(args.limit)
    return query, params


def _completed_command(args: argparse.Namespace) -> int:
    settings = get_settings()
    if not settings.postgres_dsn:
        print("✗ JOBRELAY_POSTGRES_DSN must be set")
        return 1
    manager = JobRelayPostgresManager(settings)

    query = """
        SELECT CAST(job_id AS VARCHAR) AS job_id,
               job_type,
               miner_hotkey,
               status,
               completed_at,
               execution_duration_ms,
               result_image_url,
               audit_status,
               verification_status,
               payload,
               response_payload
        FROM inference_jobs
        WHERE completed_at IS NOT NULL AND status <> 'pending'
    """
    clauses = []
    params: list[Any] = []
    if args.job_type:
        clauses.append("job_type = %s")
        params.append(args.job_type)
    if args.status:
        placeholders = ", ".join("%s" for _ in args.status)
        clauses.append(f"status IN ({placeholders})")
        params.extend(args.status)
    if args.hotkey:
        clauses.append("miner_hotkey = %s")
        params.append(args.hotkey)
    if args.since:
        clauses.append("completed_at >= %s")
        params.append(args.since)
    if clauses:
        query += " AND " + " AND ".join(clauses)
    query += " ORDER BY completed_at DESC, job_id"
    if args.limit:
        query += " LIMIT %s"
        params.append(args.limit)

    with manager._pool.connection() as conn:  # noqa: SLF001 - reuse pool for CLI
        cursor = conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    records = _rowdicts(columns, rows)

    if not args.show_payload:
        for row in records:
            row.pop("payload", None)
            row.pop("response_payload", None)

    if args.output == "json":
        _emit_json(records)
    else:
        _emit_table(records, include_payload=args.show_payload)
    return 0


def _flush_command() -> int:
    settings = get_settings()
    if not settings.postgres_dsn:
        print("✗ JOBRELAY_POSTGRES_DSN must be set for flush")
        return 1
    manager = JobRelayPostgresManager(settings)
    gcs_uri = manager.flush()
    if gcs_uri:
        print(f"✓ Flushed jobs to {gcs_uri}")
    else:
        print("✓ No jobs needed flushing")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="JobRelay utilities (Postgres-backed).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    flush_parser = subparsers.add_parser(
        "flush",
        help="Flush completed/failed jobs from Postgres to object storage and delete them.",
    )
    flush_parser.set_defaults(func=lambda _args: _flush_command())

    completed_parser = subparsers.add_parser(
        "completed",
        help="List completed jobs filtered by job type / status.",
    )
    completed_parser.add_argument(
        "--job-type",
        help="Filter by job_type (default: img-h100_pcie).",
        default="img-h100_pcie",
    )
    completed_parser.add_argument(
        "--status",
        choices=[status.value for status in JobStatus if status is not JobStatus.pending],
        action="append",
        help="Filter by completion status. Repeat flag to match multiple statuses.",
    )
    completed_parser.add_argument(
        "--hotkey",
        help="Limit results to a single miner hotkey.",
    )
    completed_parser.add_argument(
        "--since",
        type=_parse_timestamp,
        help="Only include jobs with completed_at >= timestamp (ISO 8601).",
    )
    completed_parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum rows to return (default: 25). Use 0 to return every match.",
    )
    completed_parser.add_argument(
        "--output",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table).",
    )
    completed_parser.add_argument(
        "--show-payload",
        action="store_true",
        help="Include payload and response payload columns.",
    )
    completed_parser.set_defaults(func=_completed_command)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
