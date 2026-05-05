"""Shared protocol helpers used by both async and sync clients."""

import logging
from typing import Any, NamedTuple, NoReturn, TypedDict

log = logging.getLogger("dflockd-client")

_MAX_LINE_LEN = 1_048_576  # 1 MiB — guard against unbounded server responses
_CONNECT_TIMEOUT_S = 10
_DEFAULT_HEARTBEAT_INTERVAL_S = 15.0
# Slack added to the user-supplied acquire/wait timeout when arming a
# socket-level read deadline. The server enforces the protocol timeout
# itself; the slack just bounds the case where the server hangs but TCP
# stays open. Matches the +30s used by sync_client.
_IO_TIMEOUT_SLACK_S = 30
_MAX_PROTOCOL_LINE_LEN = 256
_MAX_PAYLOAD_LEN = 64 * 1024
_MAX_PROTOCOL_SECONDS = 9_223_372_036


class DflockdError(RuntimeError):
    """Base class for protocol status errors returned by dflockd."""


class DflockdTimeoutError(TimeoutError):
    """The server returned a protocol timeout response."""


class AuthError(DflockdError):
    """Authentication was rejected by the server."""


class MaxLocksError(DflockdError):
    """The server-side max-locks limit was reached."""


class MaxWaitersError(DflockdError):
    """The server-side max-waiters limit was reached."""


class LimitMismatchError(DflockdError):
    """A semaphore operation used a different limit for an existing key."""


class NotQueuedError(DflockdError):
    """Wait was called without a matching queued request."""


class AlreadyQueuedError(DflockdError):
    """The connection is already queued for the key."""


class LeaseExpiredError(DflockdError):
    """The queued or held lease expired on the server."""


class DrainingError(DflockdError):
    """The server is draining and rejected the operation."""


_STATUS_ERRORS: dict[str, type[DflockdError]] = {
    "error_auth": AuthError,
    "error_max_locks": MaxLocksError,
    "error_max_waiters": MaxWaitersError,
    "error_limit_mismatch": LimitMismatchError,
    "error_not_enqueued": NotQueuedError,
    "error_already_enqueued": AlreadyQueuedError,
    "error_lease_expired": LeaseExpiredError,
    "error_draining": DrainingError,
}


def _byte_len(value: str) -> int:
    return len(value.encode("utf-8"))


def _check_no_newlines(name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} must not contain newlines")


def _validate_protocol_line(
    name: str, value: str, *, allow_empty: bool = True
) -> None:
    if not allow_empty and value == "":
        raise ValueError(f"{name} must not be empty")
    _check_no_newlines(name, value)
    if _byte_len(value) > _MAX_PROTOCOL_LINE_LEN:
        raise ValueError(
            f"{name} too long (max {_MAX_PROTOCOL_LINE_LEN} bytes)"
        )


def _validate_key(name: str, value: str) -> None:
    _validate_protocol_line(name, value, allow_empty=False)
    if any(c in value for c in (" ", "\t", "\n", "\r")):
        raise ValueError(f"{name} must not contain whitespace")


def _validate_token(token: str) -> None:
    _validate_protocol_line("token", token, allow_empty=False)
    if any(c in token for c in (" ", "\t")):
        raise ValueError("token must not contain whitespace")


def _validate_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")


def _validate_semaphore_limit(limit: int) -> None:
    _validate_int("limit", limit)
    if limit <= 0:
        raise ValueError("limit must be > 0")


def _validate_timeout_s(name: str, value: int) -> None:
    _validate_int(name, value)
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    if value > _MAX_PROTOCOL_SECONDS:
        raise ValueError(f"{name} too large (max {_MAX_PROTOCOL_SECONDS})")


def _validate_lease_ttl_s(value: int | None) -> None:
    if value is None:
        return
    _validate_int("lease_ttl_s", value)
    if value <= 0:
        raise ValueError("lease_ttl_s must be > 0")
    if value > _MAX_PROTOCOL_SECONDS:
        raise ValueError(f"lease_ttl_s too large (max {_MAX_PROTOCOL_SECONDS})")


def _max_signal_payload_len(channel: str) -> int:
    return _MAX_PAYLOAD_LEN - len("sig ") - _byte_len(channel) - len(" ")


def _validate_signal_channel(channel: str) -> None:
    _validate_key("channel", channel)
    if "*" in channel or ">" in channel:
        raise ValueError("channel must not contain wildcards (* or >)")


def _validate_signal_payload(channel: str, payload: str) -> None:
    _check_no_newlines("payload", payload)
    if payload.strip() == "":
        raise ValueError("payload must not be empty")
    max_payload = _max_signal_payload_len(channel)
    if max_payload < 0:
        max_payload = 0
    if _byte_len(payload) > max_payload:
        raise ValueError(f"payload too large (max {max_payload} bytes)")


def _validate_auth_token(token: str) -> None:
    _check_no_newlines("auth token", token)
    if _byte_len(token) > _MAX_PAYLOAD_LEN:
        raise ValueError(f"auth token too long (max {_MAX_PAYLOAD_LEN} bytes)")


def _raise_status_error(operation: str, resp: str) -> NoReturn:
    exc_cls = _STATUS_ERRORS.get(resp, DflockdError)
    raise exc_cls(f"{operation} failed: {resp!r}")


def _raise_auth_error(resp: str) -> NoReturn:
    if resp == "error_draining":
        _raise_status_error("authentication", resp)
    raise PermissionError(f"authentication failed: {resp!r}")


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
        _validate_semaphore_limit(limit)
    elif limit is not None:
        raise ValueError("limit must not be set when cmd_prefix='' (lock)")


def parse_lease(parts: list[str]) -> int:
    if len(parts) != 3:
        raise RuntimeError(f"bad {parts[0] if parts else 'ok'} response: {parts!r}")
    try:
        return int(parts[2])
    except ValueError as e:
        raise RuntimeError(f"bad {parts[0]} response: {parts!r}") from e


def parse_token_lease(resp: str, status: str) -> tuple[str, int]:
    parts = resp.split()
    if len(parts) != 3 or parts[0] != status:
        raise RuntimeError(f"bad {status} response: {resp!r}")
    try:
        lease = int(parts[2])
    except ValueError as e:
        raise RuntimeError(f"bad {status} response: {resp!r}") from e
    return parts[1], lease


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
