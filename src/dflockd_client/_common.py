"""Shared protocol helpers used by both async and sync clients."""

import logging
from typing import TypedDict

log = logging.getLogger("dflockd-client")


def encode_lines(*lines: str) -> bytes:
    return ("".join(f"{ln}\n" for ln in lines)).encode("utf-8")


def parse_lease(parts: list[str]) -> int:
    if len(parts) < 3:
        log.warning(
            "server did not return lease in response %r, defaulting to 30s", parts
        )
        return 30
    try:
        return int(parts[2])
    except ValueError:
        log.warning("non-integer lease in response %r, defaulting to 30s", parts)
        return 30


class StatsResult(TypedDict):
    connections: int
    locks: list[dict]
    semaphores: list[dict]
    idle_locks: list[str]
    idle_semaphores: list[str]
