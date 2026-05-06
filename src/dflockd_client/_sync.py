"""Synchronous client: ``SyncConn`` transport, low-level command functions,
and the ``DistributedLock`` / ``DistributedSemaphore`` high-level types.

The transport is intentionally thin: ``SyncConn.command`` is the only I/O
method, sending one ``cmd/key/arg`` triple and reading one response line.
Every protocol function is a 3- to 5-line wrapper:

  1. validate inputs (delegating to :mod:`_protocol`),
  2. ``conn.command(...)``,
  3. parse the response (delegating to :mod:`_protocol`).

The high-level types own a ``SyncConn`` and a background renewal thread.
The thread takes ``_io_lock`` while sending each renew so it never races
with an explicit ``release()`` on the same connection.
"""

from __future__ import annotations

import logging
import socket
import ssl
import threading
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Callable
from dataclasses import KW_ONLY, dataclass, field
from typing import Any

from . import _protocol as proto
from .errors import DflockdTimeoutError
from .sharding import (
    DEFAULT_SERVERS,
    ShardingStrategy,
    _validate_shard_index,
    stable_hash_shard,
)

log = logging.getLogger("dflockd_client")

_CONNECT_TIMEOUT_S: float = 10.0
# Slack added to the user-supplied acquire/wait timeout when arming a
# socket-level read deadline. The server enforces the protocol timeout
# itself; the slack just bounds the case where the server hangs but TCP
# stays open.
_IO_SLACK_S: float = 30.0
_RENEW_READ_TIMEOUT_S: float = 5.0
_RELEASE_READ_TIMEOUT_S: float = _IO_SLACK_S
# Fallback when self._lease is unknown — we still need *some* interval.
_DEFAULT_LEASE_FALLBACK_S = 30


# ---------------------------------------------------------------------------
# Transport: SyncConn
# ---------------------------------------------------------------------------


class SyncConn:
    """One TCP/TLS connection to a dflockd server.

    ``command`` is the only I/O method: it writes a 3-line frame and
    returns one response line. An internal mutex serialises concurrent
    callers so request/response bytes never interleave on the wire — a
    fleet of threads can hold many keys under a single ``SyncConn``.
    """

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._rfile = sock.makefile("rb")
        self._mu = threading.Lock()

    def command(
        self, cmd: str, key: str, arg: str, *, read_timeout: float | None
    ) -> str:
        frame = proto.encode_lines(cmd, key, arg)
        with self._mu:
            try:
                self._sock.settimeout(read_timeout)
                self._sock.sendall(frame)
                return self._read_line()
            except BaseException:
                self.close()
                raise

    def _read_line(self) -> str:
        raw = self._rfile.readline(proto.MAX_RESPONSE_LINE_BYTES + 1)
        if raw == b"":
            raise ConnectionError("server closed connection")
        return _trim_response_line(raw)

    def close(self) -> None:
        _close_quietly(self._rfile.close)
        _close_quietly(self._sock.close)

    def shutdown_read(self) -> None:
        """Wake any blocking read on the socket. Safe to call from another
        thread — used by ``close()`` to interrupt a renew loop's read."""
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


def _trim_response_line(raw: bytes) -> str:
    if len(raw) > proto.MAX_RESPONSE_LINE_BYTES:
        raise RuntimeError(f"server response too large ({len(raw)} bytes)")
    return raw.decode("utf-8").rstrip("\r\n")


def _close_quietly(close: Callable[[], None]) -> None:
    try:
        close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Connect + auth
# ---------------------------------------------------------------------------


def open_conn(
    host: str,
    port: int,
    *,
    ssl_context: ssl.SSLContext | None,
    connect_timeout_s: float,
) -> SyncConn:
    """Dial ``host:port`` (optionally over TLS) and return a ``SyncConn``.

    The returned conn has ``connect_timeout_s`` set as its initial socket
    timeout; protocol functions override it per call.
    """
    sock = _open_socket(host, port, connect_timeout_s, ssl_context)
    return SyncConn(sock)


def _open_socket(
    host: str,
    port: int,
    connect_timeout_s: float,
    ssl_context: ssl.SSLContext | None,
) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=connect_timeout_s)
    try:
        return _wrap_ssl(sock, host, ssl_context, connect_timeout_s)
    except BaseException:
        sock.close()
        raise


def _wrap_ssl(
    sock: socket.socket,
    host: str,
    ssl_context: ssl.SSLContext | None,
    connect_timeout_s: float,
) -> socket.socket:
    if ssl_context is not None:
        sock = ssl_context.wrap_socket(sock, server_hostname=host)
    sock.settimeout(connect_timeout_s)
    return sock


def authenticate(conn: SyncConn, auth_token: str) -> None:
    """Send the ``auth`` command. Raises ``PermissionError`` on rejection."""
    proto.validate_auth_token(auth_token)
    resp = conn.command("auth", "_", auth_token, read_timeout=_IO_SLACK_S)
    proto.parse_auth_response(resp)


# ---------------------------------------------------------------------------
# Low-level protocol functions
# ---------------------------------------------------------------------------


def acquire(
    conn: SyncConn,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    limit: int | None = None,
) -> tuple[str, int]:
    """Single-phase acquire. Returns ``(token, lease_seconds)`` on grant,
    raises :class:`DflockdTimeoutError` on server-side timeout."""
    proto.validate_prefix_limit(prefix, limit)
    proto.validate_key("key", key)
    arg = proto.make_acquire_arg(
        acquire_timeout_s, limit=limit, lease_ttl_s=lease_ttl_s
    )
    resp = conn.command(
        proto.cmd_name(prefix, "l"),
        key,
        arg,
        read_timeout=acquire_timeout_s + _IO_SLACK_S,
    )
    return proto.parse_grant_response(resp, op=proto.op_label(prefix, "acquire"))


def release(conn: SyncConn, key: str, token: str, *, prefix: str = "") -> None:
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    proto.validate_token(token)
    resp = conn.command(
        proto.cmd_name(prefix, "r"), key, token, read_timeout=_RELEASE_READ_TIMEOUT_S
    )
    proto.parse_release_response(resp, op=proto.op_label(prefix, "release"))


def renew(
    conn: SyncConn,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    read_timeout: float = _IO_SLACK_S,
) -> int:
    """Returns the remaining seconds on the lease."""
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    arg = proto.make_renew_arg(token, lease_ttl_s)
    resp = conn.command(
        proto.cmd_name(prefix, "n"), key, arg, read_timeout=read_timeout
    )
    return proto.parse_renew_response(resp, op=proto.op_label(prefix, "renew"))


def enqueue(
    conn: SyncConn,
    key: str,
    lease_ttl_s: int | None = None,
    *,
    prefix: str = "",
    limit: int | None = None,
) -> tuple[str, str | None, int | None]:
    """Two-phase phase 1. Returns ``("acquired", token, lease)`` (fast path)
    or ``("queued", None, None)`` (caller must follow with :func:`wait`)."""
    proto.validate_prefix_limit(prefix, limit)
    proto.validate_key("key", key)
    arg = proto.make_enqueue_arg(limit=limit, lease_ttl_s=lease_ttl_s)
    resp = conn.command(proto.cmd_name(prefix, "e"), key, arg, read_timeout=_IO_SLACK_S)
    return proto.parse_enqueue_response(resp, op=proto.op_label(prefix, "enqueue"))


def wait(
    conn: SyncConn,
    key: str,
    wait_timeout_s: int,
    *,
    prefix: str = "",
) -> tuple[str, int]:
    """Two-phase phase 2 — block server-side up to ``wait_timeout_s`` seconds."""
    proto.validate_prefix(prefix)
    proto.validate_key("key", key)
    arg = proto.make_wait_arg(wait_timeout_s)
    resp = conn.command(
        proto.cmd_name(prefix, "w"),
        key,
        arg,
        read_timeout=wait_timeout_s + _IO_SLACK_S,
    )
    return proto.parse_grant_response(resp, op=proto.op_label(prefix, "wait"))


def stats(conn: SyncConn) -> proto.StatsResult:
    resp = conn.command("stats", "_", "_", read_timeout=_IO_SLACK_S)
    return proto.parse_stats_response(resp)


# ---------------------------------------------------------------------------
# Semaphore convenience wrappers (delegate to lock variants with ``prefix='s'``)
# ---------------------------------------------------------------------------


def sem_acquire(
    conn: SyncConn,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    return acquire(conn, key, acquire_timeout_s, lease_ttl_s, prefix="s", limit=limit)


def sem_release(conn: SyncConn, key: str, token: str) -> None:
    release(conn, key, token, prefix="s")


def sem_renew(
    conn: SyncConn,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    return renew(conn, key, token, lease_ttl_s, prefix="s")


def sem_enqueue(
    conn: SyncConn,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    return enqueue(conn, key, lease_ttl_s, prefix="s", limit=limit)


def sem_wait(conn: SyncConn, key: str, wait_timeout_s: int) -> tuple[str, int]:
    return wait(conn, key, wait_timeout_s, prefix="s")


# ---------------------------------------------------------------------------
# Renewal loop helpers (pure-ish; ``DistributedLock`` orchestrates them)
# ---------------------------------------------------------------------------


def renew_interval(lease_s: int, ratio: float) -> float:
    """Sleep this many seconds between renews."""
    lease = lease_s if lease_s > 0 else _DEFAULT_LEASE_FALLBACK_S
    return max(1.0, lease * ratio)


# ---------------------------------------------------------------------------
# High-level: shared base
# ---------------------------------------------------------------------------


@dataclass
class _SyncBase(metaclass=ABCMeta):
    """Shared lifecycle for sync ``DistributedLock`` and ``DistributedSemaphore``.

    Subclasses override ``_proto_*`` with the appropriate prefix variants.
    """

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

    _conn: SyncConn | None = field(default=None, init=False, repr=False)
    token: str | None = field(default=None, init=False)
    lease: int = field(default=0, init=False)
    _renew_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )
    _io_lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _state_lock: Any = field(default_factory=threading.RLock, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        proto.validate_key("key", self.key)
        proto.validate_timeout_s("acquire_timeout_s", self.acquire_timeout_s)
        proto.validate_lease_ttl_s(self.lease_ttl_s)
        _validate_servers(self.servers)
        _validate_renew_ratio(self.renew_ratio)

    def __del__(self) -> None:
        _warn_if_leaked_conn(self)

    # --- protocol hooks ------------------------------------------------------

    @abstractmethod
    def _proto_acquire(self, conn: SyncConn) -> tuple[str, int]: ...

    @abstractmethod
    def _proto_release(self, conn: SyncConn, token: str) -> None: ...

    @abstractmethod
    def _proto_renew(self, conn: SyncConn, token: str) -> int: ...

    @abstractmethod
    def _proto_enqueue(self, conn: SyncConn) -> tuple[str, str | None, int | None]: ...

    @abstractmethod
    def _proto_wait(self, conn: SyncConn, timeout_s: int) -> tuple[str, int]: ...

    # --- public API ----------------------------------------------------------

    def acquire(self) -> bool:
        with self._state_lock:
            return self._acquire_unlocked()

    def enqueue(self) -> str:
        with self._state_lock:
            return self._enqueue_unlocked()

    def wait(self, timeout_s: int | None = None) -> bool:
        with self._state_lock:
            return self._wait_unlocked(timeout_s)

    def release(self) -> bool:
        with self._state_lock:
            return self._release_unlocked()

    def close(self) -> None:
        with self._state_lock:
            self._close_unlocked()

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"timeout acquiring {self.key!r}")
        return self

    def __exit__(self, *_: object) -> None:
        try:
            self.release()
        except Exception:
            log.warning("release failed in __exit__: key=%s", self.key, exc_info=True)

    # --- single-phase acquire ------------------------------------------------

    def _acquire_unlocked(self) -> bool:
        self._reset_for_new_attempt()
        self._open_and_authenticate()
        return self._invoke_acquire()

    def _invoke_acquire(self) -> bool:
        try:
            self.token, self.lease = self._proto_acquire(self._require_conn())
        except DflockdTimeoutError:
            self._close_unlocked()
            return False
        except BaseException:
            self._close_unlocked()
            raise
        self._start_renew()
        return True

    # --- two-phase enqueue ---------------------------------------------------

    def _enqueue_unlocked(self) -> str:
        self._reset_for_new_attempt()
        self._open_and_authenticate()
        return self._invoke_enqueue()

    def _invoke_enqueue(self) -> str:
        try:
            status, tok, lease = self._proto_enqueue(self._require_conn())
        except BaseException:
            self._close_unlocked()
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

    def _wait_unlocked(self, timeout_s: int | None) -> bool:
        if self.token is not None:
            return True
        timeout = timeout_s if timeout_s is not None else self.acquire_timeout_s
        return self._invoke_wait(timeout)

    def _invoke_wait(self, timeout_s: int) -> bool:
        conn = self._require_conn_for_wait()
        try:
            self.token, self.lease = self._proto_wait(conn, timeout_s)
        except DflockdTimeoutError:
            self._close_unlocked()
            return False
        except BaseException:
            self._close_unlocked()
            raise
        self._start_renew()
        return True

    def _require_conn_for_wait(self) -> SyncConn:
        if self._conn is None:
            raise RuntimeError("not connected; call enqueue() first")
        return self._conn

    # --- release -------------------------------------------------------------

    def _release_unlocked(self) -> bool:
        self._stop_renew()
        with self._io_lock:
            released = self._send_release_quietly()
        self._close_unlocked()
        return released

    def _send_release_quietly(self) -> bool:
        if self._conn is None or self.token is None:
            return False
        return self._safe_release(self._conn, self.token)

    def _safe_release(self, conn: SyncConn, token: str) -> bool:
        try:
            self._proto_release(conn, token)
            return True
        except Exception:
            log.warning(
                "%s explicit release failed (lease will expire server-side): key=%s",
                type(self).__name__,
                self.key,
                exc_info=True,
            )
            return False

    # --- close & reset -------------------------------------------------------

    def _close_unlocked(self) -> None:
        if self._closed:
            return
        self._begin_close()
        self._tear_down_conn_and_thread()

    def _begin_close(self) -> None:
        self._closed = True
        self._stop_event.set()

    def _tear_down_conn_and_thread(self) -> None:
        self._shutdown_and_close_conn()
        self._join_renew_thread()
        self._clear_held_state()

    def _shutdown_and_close_conn(self) -> None:
        conn = self._conn
        if conn is None:
            return
        conn.shutdown_read()
        with self._io_lock:
            conn.close()
        self._conn = None

    def _join_renew_thread(self) -> None:
        t = self._renew_thread
        if t is None or t is threading.current_thread():
            self._renew_thread = None
            return
        t.join(timeout=5)
        self._renew_thread = None

    def _clear_held_state(self) -> None:
        self.token = None
        self.lease = 0

    def _reset_for_new_attempt(self) -> None:
        """Tear down any old conn/thread, then re-arm for a new acquire/enqueue."""
        self._stop_renew()
        self._close_unlocked()
        self._closed = False
        self._stop_event = threading.Event()

    # --- connection management ----------------------------------------------

    def _open_and_authenticate(self) -> None:
        host, port = self._pick_server()
        conn = open_conn(
            host,
            port,
            ssl_context=self.ssl_context,
            connect_timeout_s=self.connect_timeout_s,
        )
        self._conn = _maybe_authenticate(conn, self.auth_token)

    def _pick_server(self) -> tuple[str, int]:
        idx = _validate_shard_index(
            self.sharding_strategy(self.key, len(self.servers)),
            len(self.servers),
        )
        return self.servers[idx]

    def _require_conn(self) -> SyncConn:
        if self._conn is None:
            raise RuntimeError("not connected")
        return self._conn

    # --- renew thread --------------------------------------------------------

    def _start_renew(self) -> None:
        stop_event = self._stop_event
        self._renew_thread = threading.Thread(
            target=self._renew_loop,
            args=(stop_event,),
            daemon=True,
            name=f"dflockd-renew[{self.key}]",
        )
        self._renew_thread.start()

    def _stop_renew(self) -> None:
        if self._renew_thread is None:
            return
        self._stop_event.set()
        if self._renew_thread is not threading.current_thread():
            self._renew_thread.join(timeout=5)
        self._renew_thread = None

    def _renew_loop(self, stop_event: threading.Event | None = None) -> None:
        active_stop_event = stop_event if stop_event is not None else self._stop_event
        interval = renew_interval(self.lease, self.renew_ratio)
        while not active_stop_event.wait(interval):
            remaining = self._renew_tick(active_stop_event)
            if remaining is None or active_stop_event.is_set():
                return
            interval = renew_interval(remaining, self.renew_ratio)
            self._update_lease(remaining)

    def _renew_tick(self, stop_event: threading.Event | None = None) -> int | None:
        active_stop_event = stop_event if stop_event is not None else self._stop_event
        with self._io_lock:
            if self._closed or active_stop_event.is_set():
                return None
            return self._safe_renew_once(active_stop_event)

    def _safe_renew_once(self, stop_event: threading.Event | None = None) -> int | None:
        conn, token = self._conn, self.token
        if conn is None or token is None:
            return None
        try:
            return self._proto_renew(conn, token)
        except Exception:
            self._handle_renew_failure(conn, stop_event)
            return None

    def _handle_renew_failure(
        self, conn: SyncConn, stop_event: threading.Event | None = None
    ) -> None:
        self._log_renew_failure(stop_event)
        if self._conn is conn:
            self._conn = None
            self._clear_held_state()
            conn.close()

    def _log_renew_failure(self, stop_event: threading.Event | None = None) -> None:
        active_stop_event = stop_event if stop_event is not None else self._stop_event
        if self._closed or active_stop_event.is_set():
            return
        log.error(
            "%s lost (renew failed): key=%s token=%s",
            type(self).__name__,
            self.key,
            self.token,
        )

    def _update_lease(self, remaining: int) -> None:
        if remaining > 0 and self.token is not None and not self._closed:
            self.lease = remaining


# ---------------------------------------------------------------------------
# Module-level helpers used by ``_SyncBase``
# ---------------------------------------------------------------------------


def _validate_servers(servers: list[tuple[str, int]]) -> None:
    if not servers:
        raise ValueError("servers must be a non-empty list")


def _validate_renew_ratio(ratio: float) -> None:
    if not 0 < ratio < 1:
        raise ValueError("renew_ratio must be between 0 and 1 (exclusive)")


def _maybe_authenticate(conn: SyncConn, auth_token: str | None) -> SyncConn:
    if auth_token is None:
        return conn
    try:
        authenticate(conn, auth_token)
        return conn
    except BaseException:
        conn.close()
        raise


def _warn_if_leaked_conn(obj: "_SyncBase") -> None:
    """Warn (and best-effort close) if a lock/sem is GC'd while still holding
    a connection. Called from ``__del__`` — must not raise."""
    try:
        if obj._conn is not None:
            warnings.warn(
                f"{type(obj).__name__}(key={obj.key!r}) was garbage collected "
                "without calling release() or close(). This leaks a connection.",
                ResourceWarning,
                stacklevel=2,
            )
            _close_quietly(obj._conn.close)
    except BaseException:
        pass


# ---------------------------------------------------------------------------
# DistributedLock
# ---------------------------------------------------------------------------


@dataclass
class DistributedLock(_SyncBase):
    """High-level distributed lock with automatic background lease renewal."""

    def _proto_acquire(self, conn: SyncConn) -> tuple[str, int]:
        return acquire(conn, self.key, self.acquire_timeout_s, self.lease_ttl_s)

    def _proto_release(self, conn: SyncConn, token: str) -> None:
        release(conn, self.key, token)

    def _proto_renew(self, conn: SyncConn, token: str) -> int:
        return renew(
            conn,
            self.key,
            token,
            self.lease_ttl_s,
            read_timeout=_RENEW_READ_TIMEOUT_S,
        )

    def _proto_enqueue(self, conn: SyncConn) -> tuple[str, str | None, int | None]:
        return enqueue(conn, self.key, self.lease_ttl_s)

    def _proto_wait(self, conn: SyncConn, timeout_s: int) -> tuple[str, int]:
        return wait(conn, self.key, timeout_s)


# ---------------------------------------------------------------------------
# DistributedSemaphore
# ---------------------------------------------------------------------------


@dataclass
class DistributedSemaphore(_SyncBase):
    """Multi-slot equivalent of ``DistributedLock``."""

    _: KW_ONLY
    limit: int = 0  # required; validated in __post_init__

    def __post_init__(self) -> None:
        proto.validate_semaphore_limit(self.limit)
        super().__post_init__()

    def _proto_acquire(self, conn: SyncConn) -> tuple[str, int]:
        return acquire(
            conn,
            self.key,
            self.acquire_timeout_s,
            self.lease_ttl_s,
            prefix="s",
            limit=self.limit,
        )

    def _proto_release(self, conn: SyncConn, token: str) -> None:
        release(conn, self.key, token, prefix="s")

    def _proto_renew(self, conn: SyncConn, token: str) -> int:
        return renew(
            conn,
            self.key,
            token,
            self.lease_ttl_s,
            prefix="s",
            read_timeout=_RENEW_READ_TIMEOUT_S,
        )

    def _proto_enqueue(self, conn: SyncConn) -> tuple[str, str | None, int | None]:
        return enqueue(
            conn,
            self.key,
            self.lease_ttl_s,
            prefix="s",
            limit=self.limit,
        )

    def _proto_wait(self, conn: SyncConn, timeout_s: int) -> tuple[str, int]:
        return wait(conn, self.key, timeout_s, prefix="s")
