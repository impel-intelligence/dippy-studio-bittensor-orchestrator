import asyncio
import os
import uuid
from datetime import datetime, timezone
from typing import Iterator

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.routes import create_internal_router, MinerUpsertRequest
from orchestrator.services.database_service import DatabaseService
from orchestrator.services.score_service import ScoreRecord, ScoreService


def _build_state() -> dict[str, Miner]:
    miner = Miner(
        uid=1,
        network_address="wss://entrypoint-finney.opentensor.ai:443",
        valid=True,
        alpha_stake=42,
        hotkey="hk1",
    )
    return {miner.hotkey: miner}


def _resolve_base_dsn() -> str:
    return (
        os.getenv("TEST_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or "postgresql://orchestrator:orchestrator@localhost:10069/orchestrator"
    )


def _create_temp_database(base_dsn: str) -> tuple[str, str, str]:
    params = conninfo_to_dict(base_dsn)
    temp_db_name = f"test_{uuid.uuid4().hex}"

    admin_params = params.copy()
    admin_params["dbname"] = admin_params.get("maintenance_db") or "postgres"
    admin_conninfo = make_conninfo(**admin_params)

    owner = params.get("user") or params.get("username") or None

    with psycopg.connect(admin_conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            create_stmt = sql.SQL("CREATE DATABASE {}" ).format(sql.Identifier(temp_db_name))
            if owner:
                create_stmt += sql.SQL(" OWNER {}" ).format(sql.Identifier(owner))
            cur.execute(create_stmt)

    temp_params = params.copy()
    temp_params["dbname"] = temp_db_name
    temp_conninfo = make_conninfo(**temp_params)
    return temp_conninfo, admin_conninfo, temp_db_name


def _drop_database(admin_conninfo: str, database_name: str) -> None:
    with psycopg.connect(admin_conninfo, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (database_name,),
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}" ).format(sql.Identifier(database_name)))


@pytest.fixture()
def database_service() -> Iterator[DatabaseService]:
    base_dsn = _resolve_base_dsn()
    temp_conninfo, admin_conninfo, temp_db_name = _create_temp_database(base_dsn)
    service = DatabaseService(temp_conninfo)
    try:
        yield service
    finally:
        service.close()
        _drop_database(admin_conninfo, temp_db_name)


def test_live_metagraph_client_tracks_sync_metadata(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)

    assert client.last_update() is None
    assert client.last_block() is None

    fetched_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    client.update_state(_build_state(), block=99, fetched_at=fetched_at)

    assert client.last_update() == fetched_at
    assert client.last_block() == 99


def test_metagraph_state_endpoint_includes_metadata(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)

    fetched_at = datetime(2025, 2, 15, 8, 30, tzinfo=timezone.utc)
    client.update_state(_build_state(), block=7, fetched_at=fetched_at)

    router = create_internal_router()
    target_route = next(
        route
        for route in router.routes
        if getattr(route, "path", None) == "/_internal/metagraph/state"
    )

    response = asyncio.run(target_route.endpoint(client=client))

    assert response.last_updated == fetched_at.isoformat()
    assert response.block == 7
    assert response.meta is not None
    assert response.meta["last_updated"] == fetched_at.isoformat()
    assert "***" in response.meta.get("db_path", "")


def test_miner_crud_helpers(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)

    created = client.upsert_miner(
        Miner(
            uid=2,
            network_address="http://localhost/miner",
            valid=False,
            alpha_stake=7,
            hotkey="hk-test",
        )
    )

    assert created.hotkey == "hk-test"
    fetched = client.get_miner("hk-test")
    assert fetched is not None
    assert fetched.hotkey == "hk-test"

    deleted = client.delete_miner("hk-test")
    assert deleted is True
    assert client.get_miner("hk-test") is None


def test_internal_miner_routes_support_crud(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)
    router = create_internal_router()

    post_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners" and "POST" in route.methods
    )

    payload = MinerUpsertRequest(
        hotkey="hk-route",
        uid=10,
        network_address="http://example",
        valid=True,
        alpha_stake=123,
        capacity={"foo": "bar"},
    )

    created = asyncio.run(post_route.endpoint(request=payload, client=client))
    assert created.hotkey == "hk-route"
    assert created.capacity.get("foo") == "bar"

    list_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners" and "GET" in route.methods
    )
    listing = asyncio.run(list_route.endpoint(client=client))
    assert "hk-route" in listing.miners

    get_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/{hotkey}" and "GET" in route.methods
    )
    fetched = asyncio.run(get_route.endpoint(hotkey="hk-route", client=client))
    assert fetched.hotkey == "hk-route"

    put_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/{hotkey}" and "PUT" in route.methods
    )
    update_payload = MinerUpsertRequest(
        uid=11,
        network_address="http://updated",
        valid=False,
        alpha_stake=999,
        capacity={"baz": 1},
    )
    updated = asyncio.run(
        put_route.endpoint(hotkey="hk-route", request=update_payload, client=client)
    )
    assert updated.alpha_stake == 999
    assert updated.valid is False

    delete_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/{hotkey}" and "DELETE" in route.methods
    )
    delete_response = asyncio.run(delete_route.endpoint(hotkey="hk-route", client=client))
    assert delete_response.hotkey == "hk-route"
    assert delete_response.status == "deleted"
    assert client.get_miner("hk-route") is None


def test_scores_persist_alongside_miners(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)
    score_service = ScoreService(database_service)
    hotkey = "hk-score"

    score_service.put(hotkey, ScoreRecord(scores=2.5, is_slashed=False))
    initial = score_service.get(hotkey)
    assert initial is not None
    assert initial.scores == pytest.approx(2.5)

    client.update_state(
        {
            hotkey: Miner(
                uid=3,
                network_address="http://score.example",
                valid=True,
                alpha_stake=77,
                capacity={},
                hotkey=hotkey,
            )
        },
        block=123,
    )

    preserved = score_service.get(hotkey)
    assert preserved is not None
    assert preserved.scores == pytest.approx(2.5)

    with database_service.cursor() as cur:
        cur.execute(
            "SELECT value, scores FROM miners WHERE hotkey = %s",
            (hotkey,),
        )
        row = cur.fetchone()

    assert row is not None
    miner_payload, score_payload = row
    assert miner_payload is not None
    assert score_payload is not None


class _StubResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self) -> "_StubResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_validate_state_preserves_manually_invalidated_miners(
    database_service: DatabaseService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = LiveMinerMetagraphClient(database_service)
    hotkey = "hk-audit-invalid"
    client.upsert_miner(
        Miner(
            uid=10,
            network_address="https://existing",
            valid=False,
            alpha_stake=11,
            hotkey=hotkey,
        )
    )

    def _fake_urlopen(*args, **kwargs):  # noqa: ANN001 - signature mirrors stdlib
        return _StubResponse(status=200)

    monkeypatch.setattr("orchestrator.clients.miner_metagraph.urlopen", _fake_urlopen)

    new_state = {
        hotkey: Miner(
            uid=10,
            network_address="https://example.com",
            valid=True,
            alpha_stake=11,
            hotkey=hotkey,
        )
    }

    validated = client.validate_state(new_state)
    assert validated[hotkey].valid is False


def test_validate_state_preserves_manually_validated_miners(database_service: DatabaseService) -> None:
    client = LiveMinerMetagraphClient(database_service)
    hotkey = "hk-audit-valid"
    client.upsert_miner(
        Miner(
            uid=22,
            network_address="https://existing",
            valid=True,
            alpha_stake=9,
            hotkey=hotkey,
        )
    )

    new_state = {
        hotkey: Miner(
            uid=22,
            network_address="",
            valid=False,
            alpha_stake=9,
            hotkey=hotkey,
        )
    }

    validated = client.validate_state(new_state)
    assert validated[hotkey].valid is True
