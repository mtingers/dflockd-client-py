import io
import json
import socket
import ssl
import threading
import warnings
from dataclasses import KW_ONLY, dataclass, field
from typing import TextIO

from ._common import (
    _CONNECT_TIMEOUT_S,
    _MAX_LINE_LEN,
    StatsResult,
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
) -> tuple[str, int]:
    arg = (
        str(acquire_timeout_s)
        if lease_ttl_s is None
        else f"{acquire_timeout_s} {lease_ttl_s}"
    )

    sock.sendall(encode_lines("l", key, arg))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"acquire failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


def renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    sock.sendall(encode_lines("n", key, arg))

    resp = _readline(rfile)
    if resp != "ok" and not resp.startswith("ok "):
        raise RuntimeError(f"renew failed: {resp!r}")

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
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    arg = "" if lease_ttl_s is None else str(lease_ttl_s)
    sock.sendall(encode_lines("e", key, arg))

    resp = _readline(rfile)
    if resp.startswith("acquired "):
        parts = resp.split()
        if len(parts) < 2:
            raise RuntimeError(f"bad acquired response: {resp!r}")
        token = parts[1]
        lease = parse_lease(parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"enqueue failed: {resp!r}")


def wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    """
    Two-phase wait: block until lock is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    sock.sendall(encode_lines("w", key, str(wait_timeout_s)))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"wait failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


def release(sock: socket.socket, rfile: io.TextIOWrapper, key: str, token: str) -> None:
    sock.sendall(encode_lines("r", key, token))

    resp = _readline(rfile)
    if resp != "ok":
        raise RuntimeError(f"release failed: {resp!r}")


def stats(sock: socket.socket, rfile: io.TextIOWrapper) -> StatsResult:
    sock.sendall(encode_lines("stats", "_", ""))

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
        except Exception:
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
        self._closed = False
        self._stop_event.clear()
        host, port = self._pick_server()
        sock = socket.create_connection((host, port), timeout=self.connect_timeout_s)
        try:
            if self.ssl_context is not None:
                sock = self.ssl_context.wrap_socket(sock, server_hostname=host)
            sock.settimeout(None)
            self._sock = sock
            self._rfile = self._sock.makefile("r", encoding="utf-8")
        except BaseException:
            sock.close()
            self._sock = None
            self._rfile = None
            raise
        if self.auth_token is not None:
            try:
                self._sock.sendall(encode_lines("auth", "_", self.auth_token))
                resp = _readline(self._rfile)
            except BaseException:
                self.close()
                raise
            if resp != "ok":
                self.close()
                raise PermissionError(f"authentication failed: {resp!r}")
        return self._sock, self._rfile

    def _start_renew(self):
        self._renew_thread = threading.Thread(target=self._renew_loop, daemon=True)
        self._renew_thread.start()

    def _stop_renew(self):
        if self._renew_thread is not None:
            self._stop_event.set()
            self._renew_thread.join(timeout=5)
            self._renew_thread = None

    def acquire(self) -> bool:
        sock, rfile = self._connect()
        try:
            self.token, self.lease = self._proto_acquire(sock, rfile)
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        self._start_renew()
        return True

    def enqueue(self) -> str:
        """
        Two-phase step 1: connect and enqueue. Returns "acquired" or "queued".
        Starts renew loop on fast-path acquire.
        """
        sock, rfile = self._connect()
        try:
            status, tok, lease = self._proto_enqueue(sock, rfile)
        except BaseException:
            self.close()
            raise
        if status == "acquired":
            self.token = tok
            self.lease = lease or 0
            self._start_renew()
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
        try:
            self.token, self.lease = self._proto_wait(sock, rfile, timeout)
        except TimeoutError:
            self.close()
            return False
        except BaseException:
            self.close()
            raise
        self._start_renew()
        return True

    def release(self) -> bool:
        released = False
        try:
            self._stop_renew()
            with self._io_lock:
                sock, rfile = self._sock, self._rfile
                if sock is not None and rfile is not None and self.token:
                    self._proto_release(sock, rfile, self.token)
                    released = True
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
        interval = max(1.0, self.lease * self.renew_ratio)
        while not self._stop_event.wait(interval):
            renew_failed = False
            remaining = -1
            with self._io_lock:
                if self._stop_event.is_set():
                    return
                try:
                    old_timeout = sock.gettimeout()
                    sock.settimeout(5)
                    try:
                        remaining = self._proto_renew(sock, rfile, token)
                    finally:
                        sock.settimeout(old_timeout)
                except Exception:
                    if self._stop_event.is_set():
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
# Semaphore protocol functions
# ===========================================================================


def sem_acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    # sl\nkey\n"<timeout> <limit> [<lease>]"\n
    arg = f"{acquire_timeout_s} {limit}"
    if lease_ttl_s is not None:
        arg = f"{arg} {lease_ttl_s}"

    sock.sendall(encode_lines("sl", key, arg))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring semaphore {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"sem_acquire failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


def sem_renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    # sn\nkey\n"<token> [<lease>]"\n
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    sock.sendall(encode_lines("sn", key, arg))

    resp = _readline(rfile)
    if resp != "ok" and not resp.startswith("ok "):
        raise RuntimeError(f"sem_renew failed: {resp!r}")

    parts = resp.split()
    if len(parts) >= 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return -1


def sem_enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue for semaphore: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    arg = str(limit) if lease_ttl_s is None else f"{limit} {lease_ttl_s}"
    sock.sendall(encode_lines("se", key, arg))

    resp = _readline(rfile)
    if resp.startswith("acquired "):
        parts = resp.split()
        if len(parts) < 2:
            raise RuntimeError(f"bad acquired response: {resp!r}")
        token = parts[1]
        lease = parse_lease(parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"sem_enqueue failed: {resp!r}")


def sem_wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    """
    Two-phase wait for semaphore: block until semaphore slot is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    sock.sendall(encode_lines("sw", key, str(wait_timeout_s)))

    resp = _readline(rfile)
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for semaphore {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"sem_wait failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


def sem_release(
    sock: socket.socket, rfile: io.TextIOWrapper, key: str, token: str
) -> None:
    sock.sendall(encode_lines("sr", key, token))

    resp = _readline(rfile)
    if resp != "ok":
        raise RuntimeError(f"sem_release failed: {resp!r}")


# ===========================================================================
# DistributedSemaphore
# ===========================================================================


@dataclass
class DistributedSemaphore(_SyncBase):
    limit: int

    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("limit must be > 0")
        super().__post_init__()

    def _proto_acquire(self, sock, rfile):
        return sem_acquire(
            sock,
            rfile,
            self.key,
            self.acquire_timeout_s,
            self.limit,
            self.lease_ttl_s,
        )

    def _proto_renew(self, sock, rfile, token):
        return sem_renew(sock, rfile, self.key, token, self.lease_ttl_s)

    def _proto_release(self, sock, rfile, token):
        sem_release(sock, rfile, self.key, token)

    def _proto_enqueue(self, sock, rfile):
        return sem_enqueue(sock, rfile, self.key, self.limit, self.lease_ttl_s)

    def _proto_wait(self, sock, rfile, timeout):
        return sem_wait(sock, rfile, self.key, timeout)
