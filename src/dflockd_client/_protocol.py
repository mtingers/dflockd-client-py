"""Pure wire-protocol helpers for dflockd. No I/O, no sockets, no asyncio.

Every public function is one of three kinds:

  - **Validator** (``validate_*``): raise ``ValueError``/``TypeError`` for
    inputs that would corrupt the line-based wire framing or be rejected by
    the server.
  - **Encoder** (``encode_lines``, ``build_*_arg``): produce the bytes/string
    arguments that the I/O layer will write to the socket.
  - **Decoder** (``parse_*``): take a single response line from the server
    and either return a typed value or raise the appropriate sentinel
    exception from :mod:`dflockd_client.errors`.

The split lets the I/O layer (sync or async) be a thin wrapper that calls
``send_recv`` once per protocol op, so behaviour is fully exercised by unit
tests that mock ``send_recv`` and never touch a real socket.
"""

from __future__ import annotations

import json
from typing import Any, NoReturn, TypedDict

from .errors import (
    AlreadyQueuedError,
    AuthError,
    DflockdError,
    DflockdTimeoutError,
    DrainingError,
    LeaseExpiredError,
    LimitMismatchError,
    MaxLocksError,
    MaxWaitersError,
    NotQueuedError,
)

# ---------------------------------------------------------------------------
# Limits (matches the server's protocol constants)
# ---------------------------------------------------------------------------

MAX_LINE_BYTES = 256
MAX_AUTH_TOKEN_BYTES = 64 * 1024
# stats responses ship the entire LockManager snapshot as a single JSON line,
# which scales with --max-locks. A 1 MiB cap covers >10k entries comfortably
# while still bounding worst-case memory if a malicious server returns garbage.
MAX_RESPONSE_LINE_BYTES = 1 * 1024 * 1024
MAX_PROTOCOL_SECONDS = 9_223_372_036
DEFAULT_LEASE_TTL_S = 33  # server default; used for renew-loop fallback only


# ---------------------------------------------------------------------------
# Stats response shape
# ---------------------------------------------------------------------------


class StatsResult(TypedDict):
    """Decoded ``/stats`` response. Pub/sub fields are intentionally absent."""

    connections: int
    locks: list[dict[str, Any]]
    semaphores: list[dict[str, Any]]
    idle_locks: list[dict[str, Any]]
    idle_semaphores: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def _byte_len(s: str) -> int:
    return len(s.encode("utf-8"))


def _has_whitespace(s: str) -> bool:
    return any(c in s for c in (" ", "\t", "\n", "\r"))


def _check_no_newlines(name: str, value: str) -> None:
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} must not contain newlines")


def validate_protocol_line(name: str, value: str, *, allow_empty: bool = True) -> None:
    if not allow_empty and value == "":
        raise ValueError(f"{name} must not be empty")
    _check_no_newlines(name, value)
    if _byte_len(value) > MAX_LINE_BYTES:
        raise ValueError(f"{name} too long (max {MAX_LINE_BYTES} bytes)")


def validate_key(name: str, value: str) -> None:
    validate_protocol_line(name, value, allow_empty=False)
    if _has_whitespace(value):
        raise ValueError(f"{name} must not contain whitespace")


def validate_token(token: str) -> None:
    validate_protocol_line("token", token, allow_empty=False)
    if _has_whitespace(token):
        raise ValueError("token must not contain whitespace")


def _validate_int(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")


def _validate_seconds_range(name: str, value: int, *, min_value: int) -> None:
    if value < min_value:
        raise ValueError(f"{name} must be >= {min_value}")
    if value > MAX_PROTOCOL_SECONDS:
        raise ValueError(f"{name} too large (max {MAX_PROTOCOL_SECONDS})")


def validate_timeout_s(name: str, value: int) -> None:
    _validate_int(name, value)
    _validate_seconds_range(name, value, min_value=0)


def validate_lease_ttl_s(value: int | None) -> None:
    if value is None:
        return
    _validate_int("lease_ttl_s", value)
    _validate_seconds_range("lease_ttl_s", value, min_value=1)


def validate_semaphore_limit(limit: int) -> None:
    _validate_int("limit", limit)
    if limit <= 0:
        raise ValueError("limit must be > 0")


def validate_auth_token(token: str) -> None:
    _check_no_newlines("auth token", token)
    if _byte_len(token) > MAX_AUTH_TOKEN_BYTES:
        raise ValueError(f"auth token too long (max {MAX_AUTH_TOKEN_BYTES} bytes)")


def validate_prefix(prefix: str) -> None:
    if prefix not in ("", "s"):
        raise ValueError(f"cmd_prefix must be '' or 's', got {prefix!r}")


def validate_prefix_limit(prefix: str, limit: int | None) -> None:
    """``prefix='s'`` requires a limit; ``prefix=''`` rejects one."""
    validate_prefix(prefix)
    if prefix == "s":
        _require_limit_for_semaphore(limit)
    elif limit is not None:
        raise ValueError("limit must not be set when cmd_prefix='' (lock)")


def _require_limit_for_semaphore(limit: int | None) -> None:
    if limit is None:
        raise ValueError("limit is required when cmd_prefix='s' (semaphore)")
    validate_semaphore_limit(limit)


# ---------------------------------------------------------------------------
# Encoders
# ---------------------------------------------------------------------------


def encode_lines(*lines: str) -> bytes:
    """Encode a sequence of protocol lines as ``<line>\\n…``."""
    for ln in lines:
        _check_no_newlines("protocol argument", ln)
    return ("".join(f"{ln}\n" for ln in lines)).encode("utf-8")


def build_acquire_arg(
    timeout_s: int, *, limit: int | None, lease_ttl_s: int | None
) -> str:
    parts: list[str] = [str(timeout_s)]
    _append_optional_int(parts, limit)
    _append_optional_int(parts, lease_ttl_s)
    return " ".join(parts)


def build_renew_arg(token: str, lease_ttl_s: int | None) -> str:
    if lease_ttl_s is None:
        return token
    return f"{token} {lease_ttl_s}"


def build_enqueue_arg(*, limit: int | None, lease_ttl_s: int | None) -> str:
    parts: list[str] = []
    _append_optional_int(parts, limit)
    _append_optional_int(parts, lease_ttl_s)
    return " ".join(parts)


def build_wait_arg(timeout_s: int) -> str:
    return str(timeout_s)


def _append_optional_int(parts: list[str], value: int | None) -> None:
    if value is not None:
        parts.append(str(value))


def cmd_name(prefix: str, op: str) -> str:
    """Return the wire command name (e.g. ``s`` + ``e`` → ``se``)."""
    validate_prefix(prefix)
    return prefix + op


def op_label(prefix: str, op: str) -> str:
    """Return a label used in error messages: ``"sem_acquire"`` or ``"acquire"``."""
    return f"sem_{op}" if prefix == "s" else op


# The make_*_arg helpers compose validate → build → validate-line so the I/O
# layer doesn't have to repeat the same three calls per command.


def make_acquire_arg(
    timeout_s: int, *, limit: int | None, lease_ttl_s: int | None
) -> str:
    validate_timeout_s("acquire_timeout_s", timeout_s)
    validate_lease_ttl_s(lease_ttl_s)
    arg = build_acquire_arg(timeout_s, limit=limit, lease_ttl_s=lease_ttl_s)
    validate_protocol_line("acquire argument", arg)
    return arg


def make_renew_arg(token: str, lease_ttl_s: int | None) -> str:
    validate_token(token)
    validate_lease_ttl_s(lease_ttl_s)
    arg = build_renew_arg(token, lease_ttl_s)
    validate_protocol_line("renew argument", arg)
    return arg


def make_enqueue_arg(*, limit: int | None, lease_ttl_s: int | None) -> str:
    validate_lease_ttl_s(lease_ttl_s)
    arg = build_enqueue_arg(limit=limit, lease_ttl_s=lease_ttl_s)
    validate_protocol_line("enqueue argument", arg)
    return arg


def make_wait_arg(timeout_s: int) -> str:
    validate_timeout_s("wait_timeout_s", timeout_s)
    arg = build_wait_arg(timeout_s)
    validate_protocol_line("wait argument", arg)
    return arg


# ---------------------------------------------------------------------------
# Status → exception mapping
# ---------------------------------------------------------------------------

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


def status_error(operation: str, resp: str) -> DflockdError:
    """Map a wire response line to the matching sentinel exception."""
    cls = _STATUS_ERRORS.get(resp, DflockdError)
    return cls(f"{operation} failed: {resp!r}")


def raise_status_error(operation: str, resp: str) -> NoReturn:
    raise status_error(operation, resp)


def raise_auth_error(resp: str) -> NoReturn:
    """Auth path is special: ``error_draining`` is propagated; anything
    else maps to ``PermissionError`` so callers can branch on it cleanly."""
    if resp == "error_draining":
        raise DrainingError(f"authentication failed: {resp!r}")
    raise PermissionError(f"authentication failed: {resp!r}")


# ---------------------------------------------------------------------------
# Decoders
# ---------------------------------------------------------------------------


def parse_grant_response(resp: str, *, op: str) -> tuple[str, int]:
    """Decode an acquire/wait response. Raises on timeout or error."""
    if resp == "timeout":
        raise DflockdTimeoutError(f"timeout in {op}")
    if not resp.startswith("ok "):
        raise_status_error(op, resp)
    return _decode_token_lease(resp, "ok", op)


def parse_renew_response(resp: str, *, op: str) -> int:
    """Decode an ``ok <remaining>`` renew response."""
    if not resp.startswith("ok"):
        raise_status_error(op, resp)
    return _decode_remaining(resp, op)


def parse_enqueue_response(resp: str, *, op: str) -> tuple[str, str | None, int | None]:
    """Decode an enqueue response: ``("acquired", token, lease)`` or
    ``("queued", None, None)``."""
    if resp == "queued":
        return ("queued", None, None)
    if resp.startswith("acquired "):
        token, lease = _decode_token_lease(resp, "acquired", op)
        return ("acquired", token, lease)
    raise_status_error(op, resp)


def parse_release_response(resp: str, *, op: str) -> None:
    if resp != "ok":
        raise_status_error(op, resp)


def parse_auth_response(resp: str) -> None:
    if resp != "ok":
        raise_auth_error(resp)


def parse_stats_response(resp: str) -> StatsResult:
    if not resp.startswith("ok "):
        raise_status_error("stats", resp)
    return _decode_stats_json(resp[3:])


# ---------------------------------------------------------------------------
# Internal decoder helpers
# ---------------------------------------------------------------------------


def _decode_token_lease(resp: str, status: str, op: str) -> tuple[str, int]:
    parts = resp.split()
    if len(parts) != 3 or parts[0] != status:
        raise RuntimeError(f"bad {op} response: {resp!r}")
    return parts[1], _parse_int_field(parts[2], op, resp)


def _decode_remaining(resp: str, op: str) -> int:
    parts = resp.split()
    if len(parts) != 2 or parts[0] != "ok":
        raise RuntimeError(f"bad {op} response: {resp!r}")
    return _parse_int_field(parts[1], op, resp)


def _parse_int_field(raw: str, op: str, resp: str) -> int:
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"bad {op} response: {resp!r}") from e


def _decode_stats_json(payload: str) -> StatsResult:
    try:
        return json.loads(payload)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"bad stats response: {payload!r}") from e
