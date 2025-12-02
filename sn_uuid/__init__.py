"""UUID helpers compatible with Python 3.12."""

from __future__ import annotations

import secrets
import time
import uuid

__all__ = ["uuid7"]


def uuid7() -> uuid.UUID:
    """Generate a UUIDv7 compatible identifier without external deps."""

    unix_ms = int(time.time_ns() // 1_000_000) & ((1 << 60) - 1)
    time_low = unix_ms & 0xFFFFFFFF
    time_mid = (unix_ms >> 32) & 0xFFFF
    time_hi = (unix_ms >> 48) & 0x0FFF
    time_hi_and_version = time_hi | 0x7000  # set version 7

    rand_14_bits = secrets.randbits(14)
    clock_seq_low = rand_14_bits & 0xFF
    clock_seq_hi = (rand_14_bits >> 8) & 0x3F
    clock_seq_hi_and_reserved = clock_seq_hi | 0x80  # RFC 4122 variant

    node = secrets.randbits(48)

    return uuid.UUID(
        fields=(
            time_low,
            time_mid,
            time_hi_and_version,
            clock_seq_hi_and_reserved,
            clock_seq_low,
            node,
        )
    )
