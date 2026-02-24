"""Sharding helpers for routing keys to servers."""

import zlib
from collections.abc import Callable

ShardingStrategy = Callable[[str, int], int]

DEFAULT_SERVERS: tuple[tuple[str, int], ...] = (("127.0.0.1", 6388),)


def stable_hash_shard(key: str, num_servers: int) -> int:
    """Return a server index for *key* using CRC-32.

    Unlike the built-in ``hash()``, ``zlib.crc32`` is deterministic across
    processes regardless of ``PYTHONHASHSEED``.
    """
    if num_servers <= 0:
        raise ValueError("num_servers must be > 0")
    return zlib.crc32(key.encode("utf-8")) % num_servers
