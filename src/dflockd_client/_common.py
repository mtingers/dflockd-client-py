"""Shared protocol helpers used by both async and sync clients."""

import logging
from typing import Any, NamedTuple, TypedDict

log = logging.getLogger("dflockd-client")

_MAX_LINE_LEN = 1_048_576  # 1 MiB — guard against unbounded server responses
_CONNECT_TIMEOUT_S = 10
_DEFAULT_HEARTBEAT_INTERVAL_S = 15.0
# Slack added to the user-supplied acquire/wait timeout when arming a
# socket-level read deadline. The server enforces the protocol timeout
# itself; the slack just bounds the case where the server hangs but TCP
# stays open. Matches the +30s used by sync_client.
_IO_TIMEOUT_SLACK_S = 30


def encode_lines(*lines: str) -> bytes:
    for ln in lines:
        if "\n" in ln or "\r" in ln:
            raise ValueError(f"protocol argument must not contain newlines: {ln!r}")
    return ("".join(f"{ln}\n" for ln in lines)).encode("utf-8")


def _check_cmd_prefix(cmd_prefix: str) -> None:
    """Reject cmd_prefix values that don't map to a real protocol command.
    Without this, a typo like cmd_prefix='sem' silently builds 'semn',
    'semr', etc., which the server rejects with a generic 'error' — the
    client-side check surfaces the mistake at the call site.
    """
    if cmd_prefix not in ("", "s"):
        raise ValueError(f"cmd_prefix must be '' or 's', got {cmd_prefix!r}")


def _check_cmd_prefix_limit(cmd_prefix: str, limit: int | None) -> None:
    """Enforce the cmd_prefix/limit invariant for the public acquire and
    enqueue protocol functions: the semaphore variants ('s' prefix) require
    a limit; the lock variants (empty prefix) reject one. Without this
    check, mismatched calls silently produce malformed protocol — e.g. a
    lock acquire with `limit=5` sends "l key 5" which the server parses
    as `<lease_ttl>=5`, not as a limit.
    """
    _check_cmd_prefix(cmd_prefix)
    if cmd_prefix == "s":
        if limit is None:
            raise ValueError("limit is required when cmd_prefix='s' (semaphore)")
        if limit <= 0:
            raise ValueError("limit must be > 0")
    elif limit is not None:
        raise ValueError("limit must not be set when cmd_prefix='' (lock)")


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
    # idle_locks / idle_semaphores are lists of {"key": str, "idle_s": float}
    # — they are NOT bare strings. The previous list[str] annotation
    # disagreed with the wire format the server has emitted since
    # IdleInfo was introduced.
    idle_locks: list[dict[str, Any]]
    idle_semaphores: list[dict[str, Any]]
    # signal_channels: list of {"pattern": str, "group": str (optional),
    # "listeners": int}. Added when the server began surfacing pub/sub
    # subscription stats; absent from the prior typed dict.
    signal_channels: list[dict[str, Any]]


class Signal(NamedTuple):
    """A signal received from a pub/sub channel."""

    channel: str
    payload: str
