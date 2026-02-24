"""Shared protocol helpers used by both async and sync clients."""

import logging
from typing import Any, TypedDict

log = logging.getLogger("dflockd-client")

_MAX_LINE_LEN = 1_048_576  # 1 MiB — guard against unbounded server responses
_CONNECT_TIMEOUT_S = 10


def encode_lines(*lines: str) -> bytes:
    for ln in lines:
        if "\n" in ln or "\r" in ln:
            raise ValueError(f"protocol argument must not contain newlines: {ln!r}")
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
    locks: list[dict[str, Any]]
    semaphores: list[dict[str, Any]]
    idle_locks: list[str]
    idle_semaphores: list[str]
