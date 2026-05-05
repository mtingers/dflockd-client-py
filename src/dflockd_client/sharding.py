"""Sharding helpers for routing keys to a server in a small fleet.

The CRC-32 strategy matches the Go and TypeScript clients so a heterogeneous
fleet picks the same server for any given key.
"""

import zlib
from collections.abc import Callable

ShardingStrategy = Callable[[str, int], int]

DEFAULT_SERVERS: tuple[tuple[str, int], ...] = (("127.0.0.1", 6388),)


def stable_hash_shard(key: str, num_servers: int) -> int:
    """Deterministic CRC-32 shard. Stable across processes and languages."""
    if num_servers <= 0:
        raise ValueError("num_servers must be > 0")
    return zlib.crc32(key.encode("utf-8")) % num_servers
