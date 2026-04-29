import io
import json
import queue
import socket
import ssl
import threading
import warnings
from dataclasses import KW_ONLY, dataclass, field
from typing import TextIO

from ._common import (
    _CONNECT_TIMEOUT_S,
    _DEFAULT_HEARTBEAT_INTERVAL_S,
    _MAX_LINE_LEN,
    _raise_status_error,
    Signal,
    StatsResult,
    _check_cmd_prefix,
    _check_cmd_prefix_limit,
    _validate_auth_token,
    _validate_key,
    _validate_lease_ttl_s,
    _validate_protocol_line,
    _validate_signal_channel,
    _validate_signal_payload,
    _validate_timeout_s,
    _validate_token,
    encode_lines,
    log,
    parse_lease,
)
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard


def _readline(rfile: TextIO) -> str:
    raw = rfile.readline(_MAX_LINE_LEN + 1)
    if raw == "":
        raise ConnectionError("server closed connection")
    if len(raw) > _MAX_LINE_LEN:
        raise RuntimeError(f"server response too large ({len(raw)} chars)")
    return raw.rstrip("\r\n")


# ===========================================================================
# Lock protocol functions
# ===========================================================================


def acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
    *,
    cmd_prefix: str = "",
    limit: int | None = None,
) -> tuple[str, int]:
    _check_cmd_prefix_limit(cmd_prefix, limit)
    _validate_key("key", key)
    _validate_timeout_s("acquire_timeout_s", acquire_timeout_s)
    _validate_lease_ttl_s(lease_ttl_s)
    parts = [str(acquire_timeout_s)]
    if limit is not None:
        parts.append(str(limit))
    if lease_ttl_s is not None:
        parts.append(str(lease_ttl_s))
    arg = " ".join(parts)
    _validate_protocol_line("acquire argument", arg)

    sock.sendall(encode_lines(f"{cmd_prefix}l", key, arg))

    resp = _readline(rfile)
    label = f"{'semaphore ' if cmd_prefix else ''}{key!r}"
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {label}")
    if not resp.startswith("ok "):
        func = f"{'sem_' if cmd_prefix else ''}acquire"
        _raise_status_error(func, resp)

    resp_parts = resp.split()
    if len(resp_parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = resp_parts[1]
    lease = parse_lease(resp_parts)
    return token, lease


def renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
    *,
    cmd_prefix: str = "",
) -> int:
    _check_cmd_prefix(cmd_prefix)
    _validate_key("key", key)
    _validate_token(token)
    _validate_lease_ttl_s(lease_ttl_s)
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    _validate_protocol_line("renew argument", arg)
    sock.sendall(encode_lines(f"{cmd_prefix}n", key, arg))

    resp = _readline(rfile)
    func = f"{'sem_' if cmd_prefix else ''}renew"
    if resp != "ok" and not resp.startswith("ok "):
        _raise_status_error(func, resp)

    parts = resp.split()
    if len(parts) >= 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return -1


def enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    lease_ttl_s: int | None = None,
    *,
    cmd_prefix: str = "",
    limit: int | None = None,
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    _check_cmd_prefix_limit(cmd_prefix, limit)
    _validate_key("key", key)
    _validate_lease_ttl_s(lease_ttl_s)
    parts = []
    if limit is not None:
        parts.append(str(limit))
    if lease_ttl_s is not None:
        parts.append(str(lease_ttl_s))
    arg = " ".join(parts)
    _validate_protocol_line("enqueue argument", arg)
    sock.sendall(encode_lines(f"{cmd_prefix}e", key, arg))

    resp = _readline(rfile)
    if resp.startswith("acquired "):
        resp_parts = resp.split()
        if len(resp_parts) < 2:
            raise RuntimeError(f"bad acquired response: {resp!r}")
        token = resp_parts[1]
        lease = parse_lease(resp_parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    func = f"{'sem_' if cmd_prefix else ''}enqueue"
    _raise_status_error(func, resp)


def wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
    *,
    cmd_prefix: str = "",
) -> tuple[str, int]:
    """
    Two-phase wait: block until lock/semaphore is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    _check_cmd_prefix(cmd_prefix)
    _validate_key("key", key)
    _validate_timeout_s("wait_timeout_s", wait_timeout_s)
    _validate_protocol_line("wait argument", str(wait_timeout_s))
    sock.sendall(encode_lines(f"{cmd_prefix}w", key, str(wait_timeout_s)))

    resp = _readline(rfile)
    label = f"{'semaphore ' if cmd_prefix else ''}{key!r}"
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for {label}")
    if not resp.startswith("ok "):
        func = f"{'sem_' if cmd_prefix else ''}wait"
        _raise_status_error(func, resp)

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


def release(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    *,
    cmd_prefix: str = "",
) -> None:
    _check_cmd_prefix(cmd_prefix)
    _validate_key("key", key)
    _validate_token(token)
    sock.sendall(encode_lines(f"{cmd_prefix}r", key, token))

    resp = _readline(rfile)
    func = f"{'sem_' if cmd_prefix else ''}release"
    if resp != "ok":
        _raise_status_error(func, resp)


def stats(sock: socket.socket, rfile: io.TextIOWrapper) -> StatsResult:
    sock.sendall(encode_lines("stats", "_", "_"))

    resp = _readline(rfile)
    if not resp.startswith("ok "):
        raise RuntimeError(f"stats failed: {resp!r}")

    try:
        return json.loads(resp[3:])
    except json.JSONDecodeError as e:
        raise RuntimeError(f"bad stats response: {resp!r}") from e


# ===========================================================================
# Shared base class
# ===========================================================================


@dataclass
class _SyncBase:
    """Shared lifecycle for sync distributed lock/semaphore."""

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

    _sock: socket.socket | None = field(default=None, init=False, repr=False)
    _rfile: io.TextIOWrapper | None = field(default=None, init=False, repr=False)
    token: str | None = field(default=None, init=False)
    lease: int = field(default=0, init=False)
    _renew_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _stop_event: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )
    _io_lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        if not self.servers:
            raise ValueError("servers must be a non-empty list")
        if not 0 < self.renew_ratio < 1:
            raise ValueError("renew_ratio must be between 0 and 1 (exclusive)")

    def __del__(self):
        try:
            if self._sock is not None:
                warnings.warn(
                    f"{type(self).__name__}(key={self.key!r}) was garbage collected "
                    "without calling release() or close(). This leaks a connection.",
                    ResourceWarning,
                    stacklevel=1,
                )
                # Best-effort cleanup: close socket to release the FD.
                try:
                    self._sock.close()
                except Exception:
                    pass
        except BaseException:
            pass

    # --- protocol hooks (override in subclasses) ---

    def _proto_acquire(
        self, sock: socket.socket, rfile: io.TextIOWrapper
    ) -> tuple[str, int]:
        raise NotImplementedError

    def _proto_renew(
        self, sock: socket.socket, rfile: io.TextIOWrapper, token: str
    ) -> int:
        raise NotImplementedError

    def _proto_release(
        self, sock: socket.socket, rfile: io.TextIOWrapper, token: str
    ) -> None:
        raise NotImplementedError

    def _proto_enqueue(
        self, sock: socket.socket, rfile: io.TextIOWrapper
    ) -> tuple[str, str | None, int | None]:
        raise NotImplementedError

    def _proto_wait(
        self, sock: socket.socket, rfile: io.TextIOWrapper, timeout: int
    ) -> tuple[str, int]:
        raise NotImplementedError

    # --- shared lifecycle ---

    def _pick_server(self) -> tuple[str, int]:
        idx = self.sharding_strategy(self.key, len(self.servers))
        return self.servers[idx]

    def _connect(self) -> tuple[socket.socket, io.TextIOWrapper]:
        self._stop_renew()
        self.close()
        # Reset for new connection. Order matters: close() sets both
        # _closed and _stop_event, so reset them after close().
        # Create a *new* Event rather than clearing the old one so that
        # any lingering renew thread (which still references the old,
        # already-set event) exits cleanly instead of seeing a cleared
        # event and potentially continuing.
        self._closed = False
        self._stop_event = threading.Event()
        host, port = self._pick_server()
        sock = socket.create_connection((host, port), timeout=self.connect_timeout_s)
        try:
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=host)
            sock.settimeout(self.connect_timeout_s)
            self._sock = sock
            self._rfile = self._sock.makefile("r", encoding="utf-8")
        except BaseException:
            sock.close()
            self._sock = None
            self._rfile = None
            raise
        if self.auth_token:
            _validate_auth_token(self.auth_token)
            try:
                self._sock.sendall(encode_lines("auth", "_", self.auth_token))
                resp = _readline(self._rfile)
            except BaseException:
                self.close()
                raise
            if resp != "ok":
                self.close()
                raise PermissionError(f"authentication failed: {resp!r}")
        self._sock.settimeout(None)
        return self._sock, self._rfile

    def _start_renew(self):
        self._renew_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renew_thread.start()

    def _stop_renew(self):
        if self._renew_thread is not None:
            self._stop_event.set()
            self._renew_thread.join(timeout=5)
            if self._renew_thread.is_alive():
                log.warning("renew thread did not exit within 5s: key=%s", self.key)
            self._renew_thread = None

    def acquire(self) -> bool:
        sock, rfile = self._connect()
        sock.settimeout(self.acquire_timeout_s + 30)
        try:
            self.token, self.lease = self._proto_acquire(sock, rfile)
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        sock.settimeout(None)
        self._start_renew()
        return True

    def enqueue(self) -> str:
        """
        Two-phase step 1: connect and enqueue. Returns "acquired" or "queued".
        Starts renew loop on fast-path acquire.
        """
        sock, rfile = self._connect()
        sock.settimeout(30)
        try:
            status, tok, lease = self._proto_enqueue(sock, rfile)
        except BaseException:
            self.close()
            raise
        if status == "acquired":
            self.token = tok
            self.lease = lease or 0
            sock.settimeout(None)
            self._start_renew()
        else:
            sock.settimeout(None)
        return status

    def wait(self, timeout_s: int | None = None) -> bool:
        """
        Two-phase step 2: wait for grant. Returns True if granted, False on timeout.
        If already acquired (fast path from enqueue), returns immediately.
        """
        if self.token is not None:
            return True
        sock, rfile = self._sock, self._rfile
        if sock is None or rfile is None:
            raise RuntimeError("not connected; call enqueue() first")
        timeout = timeout_s if timeout_s is not None else self.acquire_timeout_s
        sock.settimeout(timeout + 30)
        try:
            self.token, self.lease = self._proto_wait(sock, rfile, timeout)
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        sock.settimeout(None)
        self._start_renew()
        return True

    def release(self) -> bool:
        released = False
        try:
            self._stop_renew()
            with self._io_lock:
                sock, rfile = self._sock, self._rfile
                if sock is not None and rfile is not None and self.token:
                    sock.settimeout(30)
                    try:
                        self._proto_release(sock, rfile, self.token)
                        released = True
                    except Exception:
                        log.warning(
                            "%s explicit release failed (lease will expire server-side): key=%s",
                            type(self).__name__,
                            self.key,
                            exc_info=True,
                        )
        finally:
            self.close()
        return released

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError(f"timeout acquiring {self.key!r}")
        return self

    def _renew_loop(self):
        sock, rfile, token = self._sock, self._rfile, self.token
        if sock is None or rfile is None or token is None:
            return
        try:
            lease = self.lease if self.lease > 0 else 30
            interval = max(1.0, lease * self.renew_ratio)
            while not self._stop_event.wait(interval):
                renew_failed = False
                remaining = -1
                with self._io_lock:
                    if self._stop_event.is_set() or self._closed:
                        return
                    try:
                        old_timeout = sock.gettimeout()
                        sock.settimeout(5)
                        try:
                            remaining = self._proto_renew(sock, rfile, token)
                        finally:
                            sock.settimeout(old_timeout)
                    except Exception:
                        if (
                            self._stop_event.is_set()
                            or self._closed
                            or self._sock is not sock
                        ):
                            return
                        log.error(
                            "%s lost (renew failed): key=%s token=%s",
                            type(self).__name__,
                            self.key,
                            token,
                        )
                        renew_failed = True
                if renew_failed:
                    return
                if remaining > 0:
                    interval = max(1.0, remaining * self.renew_ratio)
                    # Keep self.lease in sync with the server's view —
                    # see the matching note in client.py's _renew_loop.
                    if self.token == token and not self._closed:
                        self.lease = remaining
        finally:
            del sock, rfile, token

    def __exit__(self, exc_type, exc, tb):
        try:
            self.release()
        except Exception:
            pass

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._stop_event.set()
        # Interrupt any blocking socket read (e.g. in _renew_loop) so we
        # don't stall waiting for _io_lock.
        sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        with self._io_lock:
            if self._rfile:
                try:
                    self._rfile.close()
                except Exception:
                    pass
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
            self._rfile = None
            self._sock = None
            self.token = None
            self.lease = 0
        t = self._renew_thread
        if t is not None and t is not threading.current_thread():
            t.join(timeout=5)
        self._renew_thread = None


# ===========================================================================
# DistributedLock
# ===========================================================================


@dataclass
class DistributedLock(_SyncBase):
    def _proto_acquire(self, sock, rfile):
        return acquire(sock, rfile, self.key, self.acquire_timeout_s, self.lease_ttl_s)

    def _proto_renew(self, sock, rfile, token):
        return renew(sock, rfile, self.key, token, self.lease_ttl_s)

    def _proto_release(self, sock, rfile, token):
        release(sock, rfile, self.key, token)

    def _proto_enqueue(self, sock, rfile):
        return enqueue(sock, rfile, self.key, self.lease_ttl_s)

    def _proto_wait(self, sock, rfile, timeout):
        return wait(sock, rfile, self.key, timeout)


# ===========================================================================
# Semaphore protocol wrappers (thin delegates to unified functions above)
# ===========================================================================


def sem_acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    if limit <= 0:
        raise ValueError("limit must be > 0")
    return acquire(
        sock,
        rfile,
        key,
        acquire_timeout_s,
        lease_ttl_s,
        cmd_prefix="s",
        limit=limit,
    )


def sem_renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    return renew(sock, rfile, key, token, lease_ttl_s, cmd_prefix="s")


def sem_enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    if limit <= 0:
        raise ValueError("limit must be > 0")
    return enqueue(sock, rfile, key, lease_ttl_s, cmd_prefix="s", limit=limit)


def sem_wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    return wait(sock, rfile, key, wait_timeout_s, cmd_prefix="s")


def sem_release(
    sock: socket.socket, rfile: io.TextIOWrapper, key: str, token: str
) -> None:
    release(sock, rfile, key, token, cmd_prefix="s")


# ===========================================================================
# DistributedSemaphore
# ===========================================================================


@dataclass
class DistributedSemaphore(_SyncBase):
    _: KW_ONLY
    limit: int

    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("limit must be > 0")
        super().__post_init__()

    def _proto_acquire(self, sock, rfile):
        return acquire(
            sock,
            rfile,
            self.key,
            self.acquire_timeout_s,
            self.lease_ttl_s,
            cmd_prefix="s",
            limit=self.limit,
        )

    def _proto_renew(self, sock, rfile, token):
        return renew(sock, rfile, self.key, token, self.lease_ttl_s, cmd_prefix="s")

    def _proto_release(self, sock, rfile, token):
        release(sock, rfile, self.key, token, cmd_prefix="s")

    def _proto_enqueue(self, sock, rfile):
        return enqueue(
            sock,
            rfile,
            self.key,
            self.lease_ttl_s,
            cmd_prefix="s",
            limit=self.limit,
        )

    def _proto_wait(self, sock, rfile, timeout):
        return wait(sock, rfile, self.key, timeout, cmd_prefix="s")


# ===========================================================================
# Signal protocol functions
# ===========================================================================


def sig_emit(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    channel: str,
    payload: str,
) -> int:
    """Emit a signal on a literal channel (no wildcards).

    Returns the number of listeners the signal was delivered to.
    Works on a plain sock/rfile pair without a SignalConn.
    """
    _validate_signal_channel(channel)
    _validate_signal_payload(channel, payload)
    sock.sendall(encode_lines("signal", channel, payload))
    resp = _readline(rfile)
    if not resp.startswith("ok "):
        _raise_status_error("signal", resp)
    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad signal response: {resp!r}")
    try:
        return int(parts[1])
    except ValueError as e:
        raise RuntimeError(f"bad signal response: {resp!r}") from e


# ===========================================================================
# SignalConn (sync)
# ===========================================================================


@dataclass
class SignalConn:
    """Sync pub/sub signal connection.

    Maintains a background reader thread that routes push signals to a queue
    while allowing listen/unlisten/emit commands.

    Usage::

        with SignalConn(server=("127.0.0.1", 6388)) as sc:
            sc.listen("events.>")
            sc.emit("events.user.login", "alice")
            for sig in sc:
                print(sig.channel, sig.payload)
    """

    _: KW_ONLY
    server: tuple[str, int] = ("127.0.0.1", 6388)
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = _CONNECT_TIMEOUT_S
    heartbeat_interval_s: float = _DEFAULT_HEARTBEAT_INTERVAL_S

    _sock: socket.socket | None = field(default=None, init=False, repr=False)
    _rfile: io.TextIOWrapper | None = field(default=None, init=False, repr=False)
    _read_thread: threading.Thread | None = field(default=None, init=False, repr=False)
    _heartbeat_thread: threading.Thread | None = field(
        default=None, init=False, repr=False
    )
    _heartbeat_stop: threading.Event = field(
        default_factory=threading.Event, init=False, repr=False
    )
    _sig_queue: queue.Queue[Signal | None] = field(
        default_factory=lambda: queue.Queue(maxsize=64), init=False, repr=False
    )
    _resp_queue: queue.Queue[str] = field(
        default_factory=lambda: queue.Queue(maxsize=1), init=False, repr=False
    )
    _cmd_lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)
    _dropped: int = field(default=0, init=False, repr=False)

    def __del__(self):
        try:
            if self._sock is not None:
                warnings.warn(
                    f"{type(self).__name__} was garbage collected without "
                    "calling close(). This leaks a connection.",
                    ResourceWarning,
                    stacklevel=1,
                )
                try:
                    self._sock.close()
                except Exception:
                    pass
        except BaseException:
            pass

    def connect(self) -> None:
        """Connect to the server and start the background reader thread."""
        self.close()
        self._closed = False
        self._sig_queue = queue.Queue(maxsize=64)
        self._resp_queue = queue.Queue(maxsize=1)
        self._dropped = 0
        host, port = self.server
        sock = socket.create_connection((host, port), timeout=self.connect_timeout_s)
        try:
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=host)
            sock.settimeout(self.connect_timeout_s)
            self._sock = sock
            self._rfile = sock.makefile("r", encoding="utf-8")
        except BaseException:
            sock.close()
            self._sock = None
            self._rfile = None
            raise
        if self.auth_token:
            _validate_auth_token(self.auth_token)
            try:
                self._sock.sendall(encode_lines("auth", "_", self.auth_token))
                resp = _readline(self._rfile)
            except BaseException:
                self.close()
                raise
            if resp != "ok":
                self.close()
                raise PermissionError(f"authentication failed: {resp!r}")
        self._sock.settimeout(None)
        self._read_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._read_thread.start()
        self._heartbeat_stop = threading.Event()
        if self.heartbeat_interval_s > 0:
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, daemon=True
            )
            self._heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_interval_s):
            try:
                self._send_cmd("ping", "_", "")
            except (ConnectionError, RuntimeError, OSError):
                return

    def _read_loop(self) -> None:
        try:
            while True:
                # Capture _rfile locally and return cleanly if close()
                # already nullified it. The previous `assert` could fire
                # AssertionError between iterations if close() ran on
                # the main thread, crashing the read_thread loudly
                # because AssertionError isn't in the except list.
                rfile = self._rfile
                if rfile is None:
                    return
                line = _readline(rfile)
                if line.startswith("sig "):
                    rest = line[4:]
                    idx = rest.find(" ")
                    if idx < 0:
                        continue
                    sig = Signal(channel=rest[:idx], payload=rest[idx + 1 :])
                    try:
                        self._sig_queue.put_nowait(sig)
                    except queue.Full:
                        # Slow consumer — drop and bump the counter so
                        # the user can detect lossy delivery via
                        # dropped_signals. Matches the Go client's
                        # DroppedSignals().
                        self._dropped += 1
                else:
                    try:
                        self._resp_queue.put_nowait(line)
                    except queue.Full:
                        pass
        except (ConnectionError, RuntimeError, OSError, ValueError):
            pass
        finally:
            # Ensure the sentinel is delivered even if the queue is full
            # so that ``for sig in sc:`` terminates cleanly. If we have to
            # evict the oldest signal to make room, count it as dropped —
            # otherwise dropped_signals undercounts (the user never sees
            # that signal).
            try:
                self._sig_queue.put_nowait(None)
            except queue.Full:
                try:
                    self._sig_queue.get_nowait()
                    self._dropped += 1
                except queue.Empty:
                    pass
                try:
                    self._sig_queue.put_nowait(None)
                except queue.Full:
                    pass

    def _send_cmd(self, cmd: str, key: str, arg: str) -> str:
        with self._cmd_lock:
            sock = self._sock
            if sock is None:
                raise RuntimeError("not connected; call connect() first")
            # Drain any stale response.
            try:
                self._resp_queue.get_nowait()
            except queue.Empty:
                pass
            sock.sendall(encode_lines(cmd, key, arg))
            while True:
                try:
                    return self._resp_queue.get(timeout=1)
                except queue.Empty:
                    if self._closed or (
                        self._read_thread is not None
                        and not self._read_thread.is_alive()
                    ):
                        raise ConnectionError("connection closed") from None

    def listen(self, pattern: str, *, group: str = "") -> None:
        """Subscribe to signals matching *pattern*.

        Supports NATS-style wildcards: ``*`` matches one token,
        ``>`` matches one or more trailing tokens.
        *group* enables queue-group load balancing (round-robin within group).
        """
        _validate_key("pattern", pattern)
        _validate_protocol_line("group", group)
        resp = self._send_cmd("listen", pattern, group)
        if resp != "ok":
            _raise_status_error("listen", resp)

    def unlisten(self, pattern: str, *, group: str = "") -> None:
        """Remove a signal subscription. Pattern and group must match the
        original :meth:`listen` call."""
        _validate_key("pattern", pattern)
        _validate_protocol_line("group", group)
        resp = self._send_cmd("unlisten", pattern, group)
        if resp != "ok":
            _raise_status_error("unlisten", resp)

    def emit(self, channel: str, payload: str) -> int:
        """Publish a signal on a literal channel (no wildcards).

        Returns the number of listeners the signal was delivered to.
        """
        _validate_signal_channel(channel)
        _validate_signal_payload(channel, payload)
        resp = self._send_cmd("signal", channel, payload)
        if not resp.startswith("ok "):
            _raise_status_error("signal", resp)
        parts = resp.split()
        if len(parts) < 2:
            raise RuntimeError(f"bad signal response: {resp!r}")
        try:
            return int(parts[1])
        except ValueError as e:
            raise RuntimeError(f"bad signal response: {resp!r}") from e

    @property
    def signals(self) -> queue.Queue[Signal | None]:
        """Queue of received signals. ``None`` sentinel indicates connection closed."""
        return self._sig_queue

    @property
    def dropped_signals(self) -> int:
        """Total signals dropped because the queue was full when they arrived.

        Monotonically increasing across the lifetime of the current connection;
        reset to zero on each :meth:`connect`. Use to detect slow-consumer
        conditions — non-zero means signals were lost.
        """
        return self._dropped

    def __iter__(self):
        return self._iter_signals()

    def _iter_signals(self):
        while True:
            item = self._sig_queue.get()
            if item is None:
                return
            yield item

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self) -> None:
        """Close the connection and stop the background reader thread."""
        if self._closed:
            return
        self._closed = True
        self._heartbeat_stop.set()
        sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if self._rfile is not None:
            try:
                self._rfile.close()
            except Exception:
                pass
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
        self._rfile = None
        self._sock = None
        if self._read_thread is not None:
            self._read_thread.join(timeout=5)
            self._read_thread = None
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
