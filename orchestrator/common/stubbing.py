DEST_HOTKEY = '5HauDwEzvs3MHhLooiiL7GWpxFGTuVzhwDBb5NqDyES25VLd'

from orchestrator.domain.miner import Miner

DEFAULT_AUDIT_MINER_NETWORK_ADDRESS = "https://minertest.dippy-bittensor.studio"

AUDIT_MINER = Miner(
    uid=74,
    valid=True,
    network_address=DEFAULT_AUDIT_MINER_NETWORK_ADDRESS,
    alpha_stake=100000,
    capacity={},
    hotkey="5HauDwEzvs3MHhLooiiL7GWpxFGTuVzhwDBb5NqDyES25VLd",
    failed_audits=0,
    failure_count=0,
)


def resolve_audit_miner(network_address: str | None = None) -> Miner:
    """Return the default audit miner, optionally overriding the network address."""
    address = (network_address or "").strip()
    if not address:
        return AUDIT_MINER
    return AUDIT_MINER.model_copy(update={"network_address": address})
