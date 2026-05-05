"""Asynchronous client: ``AsyncConn`` transport, low-level command functions,
and the ``DistributedLock`` / ``DistributedSemaphore`` high-level types.

Mirrors :mod:`._sync` line-for-line; the only difference is that I/O
methods are async and the renewal worker is an asyncio task instead of a
thread. The protocol layer (:mod:`._protocol`) is shared.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
import warnings
from abc import ABCMeta, abstractmethod
from dataclasses import KW_ONLY, dataclass, field

from . import _protocol as proto
from .errors import DflockdTimeoutError
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard

log = logging.getLogger("dflockd_client")

_CONNECT_TIMEOUT_S: float = 10.0
_IO_SLACK_S: float = 30.0
_RENEW_READ_TIMEOUT_S: float = 5.0
_RELEASE_READ_TIMEOUT_S: float = _IO_SLACK_S
_DEFAULT_LEASE_FALLBACK_S = 30


# ---------------------------------------------------------------------------
# Transport: AsyncConn
# ---------------------------------------------------------------------------


class AsyncConn:
    """One TCP/TLS connection. ``await conn.command(...)`` is the only I/O method.

    An internal :class:`asyncio.Lock` serialises concurrent callers so
    request/response bytes never interleave on the wire.
    """

    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._mu = asyncio.Lock()

    async def command(
        self, cmd: str, key: str, arg: str, *, read_timeout: float
    ) -> str:
        async with self._mu:
            self._writer.write(proto.encode_lines(cmd, key, arg))
            await self._writer.drain()
            return await asyncio.wait_for(self._read_line(), timeout=read_timeout)

    async def _read_line(self) -> str:
        raw = await self._reader.readline()
        if raw == b"":
            raise ConnectionError("server closed connection")
        return _trim_response_line(raw)

    async def close(self) -> None:
        try:
            self._writer.close()
        except Exception:
            pass
        with contextlib.suppress(Exception):
            await asyncio.wait_for(self._writer.wait_closed(), timeout=5)


def _trim_response_line(raw: bytes) -> str:
    if len(raw) > proto.MAX_RESPONSE_LINE_BYTES:
        raise RuntimeError(f"server response too large ({len(raw)} bytes)")
    return raw.decode("utf-8").rstrip("\r\n")


# ---------------------------------------------------------------------------
# Connect + auth
# ---------------------------------------------------------------------------


async def open_conn(
    host: str,
    port: int,
    *,
    ssl_context: ssl.SSLContext | None,
    connect_timeout_s: float,
) -> AsyncConn:
    """Pass ``limit`` so :meth:`StreamReader.readline` can buffer a large
    ``stats`` JSON line (asyncio's default is 64 KiB, which is too small)."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(
            host, port, ssl=ssl_context, limit=proto.MAX_RESPONSE_LINE_BYTES,
        ),
        timeout=connect_timeout_s,
    )
    return AsyncConn(reader, writer)


async def authenticate(conn: AsyncConn, auth_token: str) -> None:
    proto.validate_auth_token(auth_token)
    resp = await conn.command("auth", "_", auth_token, read_timeout=_IO_SLACK_S)
    proto.parse_auth_response(resp)


# ---------------------------------------------------------------------------
# Low-level protocol functions (mirror :mod:`._sync`)
# ---------------------------------------------------------------------------


async def acquire(
    conn: AsyncConn,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    limit: int | None = None,
) -> tuple[str, int]:
    proto.validate_prefix_limit(prefix, limit)
    proto.validate_key("key", key)
    arg = proto.make_acquire_arg(acquire_timeout_s, limit=limit, lease_ttl_s=lease_ttl_s)
    resp = await conn.command(
        proto.cmd_name(prefix, "l"), key, arg,
        read_timeout=acquire_timeout_s + _IO_SLACK_S,
    )
    return proto.parse_grant_response(resp, op=proto.op_label(prefix, "acquire"))


async def release(
    conn: AsyncConn, key: str, token: str, *, prefix: str = ""
) -> None:
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    proto.validate_token(token)
    resp = await conn.command(
        proto.cmd_name(prefix, "r"), key, token, read_timeout=_RELEASE_READ_TIMEOUT_S
    )
    proto.parse_release_response(resp, op=proto.op_label(prefix, "release"))


async def renew(
    conn: AsyncConn,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    read_timeout: float = _IO_SLACK_S,
) -> int:
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    arg = proto.make_renew_arg(token, lease_ttl_s)
    resp = await conn.command(
        proto.cmd_name(prefix, "n"), key, arg, read_timeout=read_timeout
    )
    return proto.parse_renew_response(resp, op=proto.op_label(prefix, "renew"))


async def enqueue(
    conn: AsyncConn,
    key: str,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    limit: int | None = None,
) -> tuple[str, str | None, int | None]:
    proto.validate_prefix_limit(prefix, limit)
    proto.validate_key("key", key)
    arg = proto.make_enqueue_arg(limit=limit, lease_ttl_s=lease_ttl_s)
    resp = await conn.command(
        proto.cmd_name(prefix, "e"), key, arg, read_timeout=_IO_SLACK_S
    )
    return proto.parse_enqueue_response(resp, op=proto.op_label(prefix, "enqueue"))


async def wait(
    conn: AsyncConn,
    key: str,
    wait_timeout_s: int,
    *,
    prefix: str = "",
) -> tuple[str, int]:
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    arg = proto.make_wait_arg(wait_timeout_s)
    resp = await conn.command(
        proto.cmd_name(prefix, "w"), key, arg,
        read_timeout=wait_timeout_s + _IO_SLACK_S,
    )
    return proto.parse_grant_response(resp, op=proto.op_label(prefix, "wait"))


async def stats(conn: AsyncConn) -> proto.StatsResult:
    resp = await conn.command("stats", "_", "_", read_timeout=_IO_SLACK_S)
    return proto.parse_stats_response(resp)


# ---------------------------------------------------------------------------
# Semaphore convenience wrappers
# ---------------------------------------------------------------------------


async def sem_acquire(
    conn: AsyncConn,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    return await acquire(
        conn, key, acquire_timeout_s, lease_ttl_s, prefix="s", limit=limit
    )


async def sem_release(conn: AsyncConn, key: str, token: str) -> None:
    await release(conn, key, token, prefix="s")


async def sem_renew(
    conn: AsyncConn,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    return await renew(conn, key, token, lease_ttl_s, prefix="s")


async def sem_enqueue(
    conn: AsyncConn,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    return await enqueue(conn, key, lease_ttl_s, prefix="s", limit=limit)


async def sem_wait(
    conn: AsyncConn, key: str, wait_timeout_s: int
) -> tuple[str, int]:
    return await wait(conn, key, wait_timeout_s, prefix="s")


# ---------------------------------------------------------------------------
# Renewal interval (shared with sync)
# ---------------------------------------------------------------------------


def renew_interval(lease_s: int, ratio: float) -> float:
    lease = lease_s if lease_s > 0 else _DEFAULT_LEASE_FALLBACK_S
    return max(1.0, lease * ratio)


# ---------------------------------------------------------------------------
# High-level: shared async base
# ---------------------------------------------------------------------------


@dataclass
class _AsyncBase(metaclass=ABCMeta):
    """Shared lifecycle for async ``DistributedLock``/``DistributedSemaphore``."""

    key: str
    _: KW_ONLY
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = field(
        default_factory=lambda: list(DEFAULT_SERVERS)
    )
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = _CONNECT_TIMEOUT_S

    _conn: AsyncConn | None = field(default=None, init=False, repr=False)
    token: str | None = field(default=None, init=False)
    lease: int = field(default=0, init=False)
    _renew_task: asyncio.Task[None] | None = field(
        default=None, init=False, repr=False
    )
    _io_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False
    )
    _state_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        _validate_servers(self.servers)
        _validate_renew_ratio(self.renew_ratio)

    def __del__(self) -> None:
        _warn_if_leaked_conn(self)

    # --- protocol hooks ------------------------------------------------------

    @abstractmethod
    async def _proto_acquire(self, conn: AsyncConn) -> tuple[str, int]: ...

    @abstractmethod
    async def _proto_release(self, conn: AsyncConn, token: str) -> None: ...

    @abstractmethod
    async def _proto_renew(self, conn: AsyncConn, token: str) -> int: ...

    @abstractmethod
    async def _proto_enqueue(
        self, conn: AsyncConn
    ) -> tuple[str, str | None, int | None]: ...

    @abstractmethod
    async def _proto_wait(
        self, conn: AsyncConn, timeout_s: int
    ) -> tuple[str, int]: ...

    # --- public API ----------------------------------------------------------

    async def acquire(self) -> bool:
        async with self._state_lock:
            return await self._acquire_unlocked()

    async def enqueue(self) -> str:
        async with self._state_lock:
            return await self._enqueue_unlocked()

    async def wait(self, timeout_s: int | None = None) -> bool:
        async with self._state_lock:
            return await self._wait_unlocked(timeout_s)

    async def release(self) -> bool:
        async with self._state_lock:
            return await self._release_unlocked()

    async def aclose(self) -> None:
        async with self._state_lock:
            await self._close_unlocked()

    async def __aenter__(self):
        if not await self.acquire():
            raise TimeoutError(f"timeout acquiring {self.key!r}")
        return self

    async def __aexit__(self, *_: object) -> None:
        try:
            await self.release()
        except Exception:
            log.warning("release failed in __aexit__: key=%s", self.key, exc_info=True)

    # --- single-phase acquire ------------------------------------------------

    async def _acquire_unlocked(self) -> bool:
        await self._reset_for_new_attempt()
        await self._open_and_authenticate()
        return await self._invoke_acquire()

    async def _invoke_acquire(self) -> bool:
        try:
            self.token, self.lease = await self._proto_acquire(self._require_conn())
        except DflockdTimeoutError:
            await self._close_unlocked()
            return False
        except BaseException:
            await self._close_unlocked()
            raise
        self._start_renew()
        return True

    # --- two-phase enqueue ---------------------------------------------------

    async def _enqueue_unlocked(self) -> str:
        await self._reset_for_new_attempt()
        await self._open_and_authenticate()
        return await self._invoke_enqueue()

    async def _invoke_enqueue(self) -> str:
        try:
            status, tok, lease = await self._proto_enqueue(self._require_conn())
        except BaseException:
            await self._close_unlocked()
            raise
        return self._handle_enqueue_status(status, tok, lease)

    def _handle_enqueue_status(
        self, status: str, tok: str | None, lease: int | None
    ) -> str:
        if status == "acquired":
            self.token, self.lease = tok, lease or 0
            self._start_renew()
        return status

    # --- two-phase wait ------------------------------------------------------

    async def _wait_unlocked(self, timeout_s: int | None) -> bool:
        if self.token is not None:
            return True
        timeout = timeout_s if timeout_s is not None else self.acquire_timeout_s
        return await self._invoke_wait(timeout)

    async def _invoke_wait(self, timeout_s: int) -> bool:
        conn = self._require_conn_for_wait()
        try:
            self.token, self.lease = await self._proto_wait(conn, timeout_s)
        except DflockdTimeoutError:
            await self._close_unlocked()
            return False
        except BaseException:
            await self._close_unlocked()
            raise
        self._start_renew()
        return True

    def _require_conn_for_wait(self) -> AsyncConn:
        if self._conn is None:
            raise RuntimeError("not connected; call enqueue() first")
        return self._conn

    # --- release -------------------------------------------------------------

    async def _release_unlocked(self) -> bool:
        await self._cancel_renew()
        async with self._io_lock:
            released = await self._send_release_quietly()
        await self._close_unlocked()
        return released

    async def _send_release_quietly(self) -> bool:
        if self._conn is None or self.token is None:
            return False
        return await self._safe_release(self._conn, self.token)

    async def _safe_release(self, conn: AsyncConn, token: str) -> bool:
        try:
            await self._proto_release(conn, token)
            return True
        except Exception:
            log.warning(
                "%s explicit release failed (lease will expire server-side): key=%s",
                type(self).__name__, self.key, exc_info=True,
            )
            return False

    # --- close & reset -------------------------------------------------------

    async def _close_unlocked(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._cancel_renew()
        await self._close_conn_quietly()
        self._clear_held_state()

    async def _close_conn_quietly(self) -> None:
        conn = self._conn
        self._conn = None
        if conn is not None:
            await conn.close()

    def _clear_held_state(self) -> None:
        self.token = None
        self.lease = 0

    async def _reset_for_new_attempt(self) -> None:
        await self._cancel_renew()
        await self._close_unlocked()
        self._closed = False

    # --- connection management ----------------------------------------------

    async def _open_and_authenticate(self) -> None:
        host, port = self._pick_server()
        conn = await open_conn(
            host, port, ssl_context=self.ssl_context,
            connect_timeout_s=self.connect_timeout_s,
        )
        self._conn = await _maybe_authenticate(conn, self.auth_token)

    def _pick_server(self) -> tuple[str, int]:
        idx = self.sharding_strategy(self.key, len(self.servers))
        return self.servers[idx]

    def _require_conn(self) -> AsyncConn:
        if self._conn is None:
            raise RuntimeError("not connected")
        return self._conn

    # --- renew task ----------------------------------------------------------

    def _start_renew(self) -> None:
        self._renew_task = asyncio.create_task(
            self._renew_loop(), name=f"dflockd-renew[{self.key}]"
        )

    async def _cancel_renew(self) -> None:
        task = self._renew_task
        if task is None or task is asyncio.current_task():
            self._renew_task = None
            return
        await _cancel_and_wait(task)
        self._renew_task = None

    async def _renew_loop(self) -> None:
        interval = renew_interval(self.lease, self.renew_ratio)
        try:
            await self._renew_loop_body(interval)
        except asyncio.CancelledError:
            return

    async def _renew_loop_body(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            remaining = await self._renew_tick()
            if remaining is None:
                return
            interval = renew_interval(remaining, self.renew_ratio)
            self._update_lease(remaining)

    async def _renew_tick(self) -> int | None:
        async with self._io_lock:
            if self._closed:
                return None
            return await self._safe_renew_once()

    async def _safe_renew_once(self) -> int | None:
        conn, token = self._conn, self.token
        if conn is None or token is None:
            return None
        try:
            return await self._proto_renew(conn, token)
        except Exception:
            self._log_renew_failure()
            return None

    def _log_renew_failure(self) -> None:
        if self._closed:
            return
        log.error(
            "%s lost (renew failed): key=%s token=%s",
            type(self).__name__, self.key, self.token,
        )

    def _update_lease(self, remaining: int) -> None:
        if remaining > 0 and self.token is not None and not self._closed:
            self.lease = remaining


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _validate_servers(servers: list[tuple[str, int]]) -> None:
    if not servers:
        raise ValueError("servers must be a non-empty list")


def _validate_renew_ratio(ratio: float) -> None:
    if not 0 < ratio < 1:
        raise ValueError("renew_ratio must be between 0 and 1 (exclusive)")


async def _maybe_authenticate(
    conn: AsyncConn, auth_token: str | None
) -> AsyncConn:
    if not auth_token:
        return conn
    try:
        await authenticate(conn, auth_token)
        return conn
    except BaseException:
        await conn.close()
        raise


async def _cancel_and_wait(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(BaseException):
        await task


def _warn_if_leaked_conn(obj: "_AsyncBase") -> None:
    """Warn (and best-effort transport-close) if an instance is GC'd while
    still holding a conn. ``__del__`` must not raise."""
    try:
        if obj._conn is not None:
            warnings.warn(
                f"{type(obj).__name__}(key={obj.key!r}) was garbage collected "
                "without calling release() or aclose(). This leaks a connection.",
                ResourceWarning, stacklevel=2,
            )
            with contextlib.suppress(Exception):
                obj._conn._writer.close()
    except BaseException:
        pass


# ---------------------------------------------------------------------------
# DistributedLock
# ---------------------------------------------------------------------------


@dataclass
class DistributedLock(_AsyncBase):
    """Async high-level distributed lock with automatic background renewal."""

    async def _proto_acquire(self, conn: AsyncConn) -> tuple[str, int]:
        return await acquire(conn, self.key, self.acquire_timeout_s, self.lease_ttl_s)

    async def _proto_release(self, conn: AsyncConn, token: str) -> None:
        await release(conn, self.key, token)

    async def _proto_renew(self, conn: AsyncConn, token: str) -> int:
        return await renew(
            conn, self.key, token, self.lease_ttl_s,
            read_timeout=_RENEW_READ_TIMEOUT_S,
        )

    async def _proto_enqueue(
        self, conn: AsyncConn
    ) -> tuple[str, str | None, int | None]:
        return await enqueue(conn, self.key, self.lease_ttl_s)

    async def _proto_wait(self, conn: AsyncConn, timeout_s: int) -> tuple[str, int]:
        return await wait(conn, self.key, timeout_s)


# ---------------------------------------------------------------------------
# DistributedSemaphore
# ---------------------------------------------------------------------------


@dataclass
class DistributedSemaphore(_AsyncBase):
    """Async multi-slot equivalent of ``DistributedLock``."""

    _: KW_ONLY
    limit: int = 0  # required; validated in __post_init__

    def __post_init__(self) -> None:
        proto.validate_semaphore_limit(self.limit)
        super().__post_init__()

    async def _proto_acquire(self, conn: AsyncConn) -> tuple[str, int]:
        return await acquire(
            conn, self.key, self.acquire_timeout_s, self.lease_ttl_s,
            prefix="s", limit=self.limit,
        )

    async def _proto_release(self, conn: AsyncConn, token: str) -> None:
        await release(conn, self.key, token, prefix="s")

    async def _proto_renew(self, conn: AsyncConn, token: str) -> int:
        return await renew(
            conn, self.key, token, self.lease_ttl_s,
            prefix="s", read_timeout=_RENEW_READ_TIMEOUT_S,
        )

    async def _proto_enqueue(
        self, conn: AsyncConn
    ) -> tuple[str, str | None, int | None]:
        return await enqueue(
            conn, self.key, self.lease_ttl_s, prefix="s", limit=self.limit
        )

    async def _proto_wait(self, conn: AsyncConn, timeout_s: int) -> tuple[str, int]:
        return await wait(conn, self.key, timeout_s, prefix="s")
