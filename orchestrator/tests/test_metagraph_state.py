from datetime import datetime, timezone

from orchestrator.clients.miner_metagraph import LiveMinerMetagraphClient, Miner
from orchestrator.routes import create_internal_router
from orchestrator.services.database_service import DatabaseService


def _build_state() -> dict[str, Miner]:
    miner = Miner(
        uid=1,
        network_address="wss://entrypoint-finney.opentensor.ai:443",
        valid=True,
        alpha_stake=42,
        hotkey="hk1",
    )
    return {miner.hotkey: miner}


def test_live_metagraph_client_tracks_sync_metadata(tmp_path) -> None:
    db_path = tmp_path / "metagraph.sqlite"
    database_service = DatabaseService(db_path)
    client = LiveMinerMetagraphClient(database_service)

    assert client.last_update() is None
    assert client.last_block() is None

    fetched_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    client.update_state(_build_state(), block=99, fetched_at=fetched_at)

    assert client.last_update() == fetched_at
    assert client.last_block() == 99


def test_metagraph_state_endpoint_includes_metadata(tmp_path) -> None:
    import asyncio

    db_path = tmp_path / "metagraph.sqlite"
    database_service = DatabaseService(db_path)
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
