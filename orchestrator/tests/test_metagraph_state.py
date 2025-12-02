import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Iterator

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from orchestrator.services.miner_metagraph_service import MinerMetagraphService
from orchestrator.domain.miner import Miner
from orchestrator.routes import create_internal_router, create_public_router, MinerUpsertRequest
from orchestrator.clients.database import PostgresClient
from orchestrator.services.score_service import ScoreRecord, ScoreService
from sn_uuid import uuid7


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
    temp_db_name = f"test_{uuid7().hex}"

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
def database_service() -> Iterator[PostgresClient]:
    base_dsn = _resolve_base_dsn()
    temp_conninfo, admin_conninfo, temp_db_name = _create_temp_database(base_dsn)
    service = PostgresClient(temp_conninfo)
    try:
        yield service
    finally:
        service.close()
        _drop_database(admin_conninfo, temp_db_name)


def test_live_metagraph_client_tracks_sync_metadata(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)

    assert client.last_update() is None
    assert client.last_block() is None

    fetched_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    client.update_state(_build_state(), block=99, fetched_at=fetched_at)

    assert client.last_update() == fetched_at
    assert client.last_block() == 99


def test_metagraph_state_endpoint_includes_metadata(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    score_service = ScoreService(database_service)
    state = _build_state()
    hotkey = next(iter(state))

    fetched_at = datetime(2025, 2, 15, 8, 30, tzinfo=timezone.utc)
    client.update_state(state, block=7, fetched_at=fetched_at)
    score_service.put(hotkey, ScoreRecord(scores=1.0, failure_count=4))

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
    assert response.data[hotkey].failure_count == 4


def test_miner_crud_helpers(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)

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


def test_internal_miner_routes_support_crud(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    score_service = ScoreService(database_service)
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
        capacity={"foo": True},
    )

    created = asyncio.run(post_route.endpoint(request=payload, client=client))
    assert created.hotkey == "hk-route"
    assert created.failure_count == 0
    assert created.capacity.get("foo") is True

    score_service.put("hk-route", ScoreRecord(scores=0.5, failure_count=6))

    list_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners" and "GET" in route.methods
    )
    listing = asyncio.run(list_route.endpoint(client=client))
    assert "hk-route" in listing.miners
    assert listing.miners["hk-route"].failure_count == 6

    get_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/{hotkey}" and "GET" in route.methods
    )
    fetched = asyncio.run(get_route.endpoint(hotkey="hk-route", client=client))
    assert fetched.hotkey == "hk-route"
    assert fetched.failure_count == 6

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
        capacity={"baz": False},
    )
    updated = asyncio.run(
        put_route.endpoint(hotkey="hk-route", request=update_payload, client=client)
    )
    assert updated.alpha_stake == 999
    assert updated.valid is False
    assert updated.failure_count == 6

    delete_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/{hotkey}" and "DELETE" in route.methods
    )
    delete_response = asyncio.run(delete_route.endpoint(hotkey="hk-route", client=client))
    assert delete_response.hotkey == "hk-route"
    assert delete_response.status == "deleted"
    assert client.get_miner("hk-route") is None


def test_internal_capacity_route_reports_capacities(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    router = create_internal_router()
    fetched_at = datetime(2025, 3, 5, 6, 7, tzinfo=timezone.utc)

    client.update_state(
        {
            "hk-cap": Miner(
                uid=5,
                network_address="http://capacity.example",
                valid=True,
                alpha_stake=50,
                capacity={"H100": True, "base-h100_pcie": True},
                hotkey="hk-cap",
            )
        },
        block=55,
        fetched_at=fetched_at,
    )

    cap_route = next(
        route
        for route in router.routes
        if route.path == "/_internal/miners/capacity" and "GET" in route.methods
    )
    response = asyncio.run(cap_route.endpoint(client=client))

    assert response.block == 55
    assert response.last_updated == fetched_at.isoformat()
    assert response.capacities["hk-cap"]["base-h100_pcie"] is True
    assert response.capacities["hk-cap"]["H100"] is True


def test_fetch_candidate_filters_by_task_type(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    client.update_state(
        {
            "hk-train-only": Miner(
                uid=6,
                network_address="http://train-only",
                valid=True,
                alpha_stake=25,
                capacity={"base-h100_pcie": True},
                hotkey="hk-train-only",
            ),
            "hk-match": Miner(
                uid=7,
                network_address="http://match",
                valid=True,
                alpha_stake=100,
                capacity={"generate": True, "img-h100_pcie": True},
                hotkey="hk-match",
            ),
        },
        block=88,
    )

    selected = client.fetch_candidate(task_type="img-h100_pcie")
    assert selected is not None
    assert selected.hotkey == "hk-match"

    assert client.fetch_candidate(task_type="non-existent-task") is None


def test_fetch_candidate_requires_true_capacity(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    client.update_state(
        {
            "hk-false": Miner(
                uid=8,
                network_address="http://nope",
                valid=True,
                alpha_stake=40,
                capacity={"img-h100_pcie": False},
                hotkey="hk-false",
            )
        },
        block=91,
    )

    assert client.fetch_candidate(task_type="img-h100_pcie") is None


class _StubEpistulaClient:
    def __init__(self, responses: dict[str, tuple[int, str]]):
        self._responses = responses

    def get_signed_request_sync(
        self,
        *,
        url: str,
        miner_hotkey: str,
        timeout: int,
    ) -> tuple[int, str]:
        return self._responses[miner_hotkey]


def test_validate_state_normalizes_capacity_and_marks_invalid_on_parse_error(
    database_service: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "hk-valid": (200, json.dumps({"base-h100_pcie": True, "img-h100_pcie": True})),
        "hk-invalid": (200, "{not-json"),
    }
    epistula = _StubEpistulaClient(responses)
    client = MinerMetagraphService(database_service, epistula_client=epistula)

    def _fake_urlopen(*args, **kwargs):  # noqa: ANN001 - matches stdlib signature
        return _StubResponse(status=200)

    monkeypatch.setattr("orchestrator.services.miner_metagraph_service.urlopen", _fake_urlopen)

    state = {
        "hk-valid": Miner(
            uid=1,
            network_address="https://valid.example",
            valid=True,
            alpha_stake=10,
            hotkey="hk-valid",
        ),
        "hk-invalid": Miner(
            uid=2,
            network_address="https://invalid.example",
            valid=True,
            alpha_stake=10,
            hotkey="hk-invalid",
        ),
    }

    validated = client.validate_state(state)

    assert validated["hk-valid"].capacity == {"base-h100_pcie": True, "img-h100_pcie": True}
    assert validated["hk-valid"].valid is True
    assert validated["hk-invalid"].valid is False
    assert validated["hk-invalid"].capacity == {}


def test_scores_persist_alongside_miners(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    score_service = ScoreService(database_service)
    hotkey = "hk-score"

    score_service.put(hotkey, ScoreRecord(scores=2.5))
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


def test_fetch_miners_exposes_failure_count(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    score_service = ScoreService(database_service)
    hotkey = "hk-penalty"

    client.update_state(
        {
            hotkey: Miner(
                uid=9,
                network_address="http://penalty.example",
                valid=True,
                alpha_stake=50,
                capacity={},
                hotkey=hotkey,
            )
        }
    )

    score_service.put(
        hotkey,
        ScoreRecord(scores=1.0, failure_count=7),
    )

    miners = client.fetch_miners()
    assert hotkey in miners
    assert miners[hotkey].failure_count == 7


def test_public_miner_status_includes_failure_count(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
    score_service = ScoreService(database_service)
    hotkey = "hk-public"

    client.update_state(
        {
            hotkey: Miner(
                uid=11,
                network_address="http://public.example",
                valid=True,
                alpha_stake=88,
                capacity={},
                hotkey=hotkey,
            )
        }
    )

    score_service.put(
        hotkey,
        ScoreRecord(scores=0.9, failure_count=3),
    )

    router = create_public_router()
    miner_status_route = next(
        route
        for route in router.routes
        if getattr(route, "path", None) == "/miner_status" and "GET" in route.methods
    )

    response = asyncio.run(miner_status_route.endpoint(client=client))
    assert response.data[hotkey].failure_count == 3


class _StubResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self) -> "_StubResponse":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_validate_state_preserves_manually_invalidated_miners(
    database_service: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MinerMetagraphService(database_service)
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

    monkeypatch.setattr("orchestrator.services.miner_metagraph_service.urlopen", _fake_urlopen)

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


def test_validate_state_preserves_manually_validated_miners(database_service: PostgresClient) -> None:
    client = MinerMetagraphService(database_service)
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
