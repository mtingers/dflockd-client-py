import asyncio
import contextlib
import json
import ssl
import warnings
from dataclasses import KW_ONLY, dataclass, field

from ._common import (
    _CONNECT_TIMEOUT_S,
    _MAX_LINE_LEN,
    StatsResult,
    encode_lines,
    log,
    parse_lease,
)
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard


async def _readline(reader: asyncio.StreamReader) -> str:
    raw = await reader.readline()
    if raw == b"":
        raise ConnectionError("server closed connection")
    if len(raw) > _MAX_LINE_LEN:
        raise RuntimeError(f"server response too large ({len(raw)} bytes)")
    return raw.decode("utf-8").rstrip("\r\n")


# ===========================================================================
# Lock protocol functions
# ===========================================================================


async def acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    # l\nkey\n"<timeout> [<lease>]"\n
    arg = (
        str(acquire_timeout_s)
        if lease_ttl_s is None
        else f"{acquire_timeout_s} {lease_ttl_s}"
    )

    writer.write(encode_lines("l", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"acquire failed: {resp!r}")

    # ok <token> <lease>
    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


async def renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    # n\nkey\n"<token> [<lease>]"\n
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    writer.write(encode_lines("n", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp != "ok" and not resp.startswith("ok "):
        raise RuntimeError(f"renew failed: {resp!r}")

    # ok <seconds_remaining> (optional)
    parts = resp.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return -1


async def enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    arg = "" if lease_ttl_s is None else str(lease_ttl_s)
    writer.write(encode_lines("e", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp.startswith("acquired "):
        parts = resp.split()
        token = parts[1]
        lease = parse_lease(parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"enqueue failed: {resp!r}")


async def wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    """
    Two-phase wait: block until lock is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    writer.write(encode_lines("w", key, str(wait_timeout_s)))
    await writer.drain()

    resp = await _readline(reader)
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"wait failed: {resp!r}")

    parts = resp.split()
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


async def release(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key: str, token: str
) -> None:
    writer.write(encode_lines("r", key, token))
    await writer.drain()

    resp = await _readline(reader)
    if resp != "ok":
        raise RuntimeError(f"release failed: {resp!r}")


async def stats(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> StatsResult:
    writer.write(encode_lines("stats", "_", ""))
    await writer.drain()

    resp = await _readline(reader)
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
class _AsyncBase:
    """Shared lifecycle for async distributed lock/semaphore."""

    key: str
    _: KW_ONLY
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None  # if None, server default
    servers: list[tuple[str, int]] = field(
        default_factory=lambda: list(DEFAULT_SERVERS)
    )
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5  # renew at lease * ratio
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = _CONNECT_TIMEOUT_S

    _reader: asyncio.StreamReader | None = field(default=None, init=False, repr=False)
    _writer: asyncio.StreamWriter | None = field(default=None, init=False, repr=False)
    token: str | None = field(default=None, init=False)
    lease: int = field(default=0, init=False)
    _renew_task: asyncio.Task | None = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        if not self.servers:
            raise ValueError("servers must be a non-empty list")
        if not 0 < self.renew_ratio < 1:
            raise ValueError("renew_ratio must be between 0 and 1 (exclusive)")

    def __del__(self):
        try:
            if self._writer is not None:
                warnings.warn(
                    f"{type(self).__name__}(key={self.key!r}) was garbage collected "
                    "without calling release() or aclose(). This leaks a connection.",
                    ResourceWarning,
                    stacklevel=1,
                )
        except Exception:
            pass

    # --- protocol hooks (override in subclasses) ---

    async def _proto_acquire(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> tuple[str, int]:
        raise NotImplementedError

    async def _proto_renew(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, token: str
    ) -> int:
        raise NotImplementedError

    async def _proto_release(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, token: str
    ) -> None:
        raise NotImplementedError

    async def _proto_enqueue(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> tuple[str, str | None, int | None]:
        raise NotImplementedError

    async def _proto_wait(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        timeout: int,
    ) -> tuple[str, int]:
        raise NotImplementedError

    # --- shared lifecycle ---

    def _pick_server(self) -> tuple[str, int]:
        idx = self.sharding_strategy(self.key, len(self.servers))
        return self.servers[idx]

    async def _cancel_renew(self):
        if self._renew_task is not None:
            self._renew_task.cancel()
            with contextlib.suppress(BaseException):
                await self._renew_task
            self._renew_task = None

    async def _connect(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        await self._cancel_renew()
        await self.aclose()
        self._closed = False
        host, port = self._pick_server()
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(
                host, port, ssl=self.ssl_context, limit=_MAX_LINE_LEN
            ),
            timeout=self.connect_timeout_s,
        )
        if self.auth_token is not None:
            self._writer.write(encode_lines("auth", "_", self.auth_token))
            await self._writer.drain()
            resp = await _readline(self._reader)
            if resp != "ok":
                await self.aclose()
                raise PermissionError(f"authentication failed: {resp!r}")
        return self._reader, self._writer

    async def acquire(self) -> bool:
        reader, writer = await self._connect()
        try:
            self.token, self.lease = await self._proto_acquire(reader, writer)
        except TimeoutError:
            await self.aclose()
            return False
        except BaseException:
            await self.aclose()
            raise
        self._renew_task = asyncio.create_task(self._renew_loop())
        return True

    async def enqueue(self) -> str:
        """
        Two-phase step 1: connect and enqueue. Returns "acquired" or "queued".
        Starts renew loop on fast-path acquire.
        """
        reader, writer = await self._connect()
        try:
            status, tok, lease = await self._proto_enqueue(reader, writer)
        except BaseException:
            await self.aclose()
            raise
        if status == "acquired":
            self.token = tok
            self.lease = lease or 0
            self._renew_task = asyncio.create_task(self._renew_loop())
        return status

    async def wait(self, timeout_s: int | None = None) -> bool:
        """
        Two-phase step 2: wait for grant. Returns True if granted, False on timeout.
        If already acquired (fast path from enqueue), returns immediately.
        """
        if self.token is not None:
            # Already acquired during enqueue
            return True
        if self._reader is None or self._writer is None:
            raise RuntimeError("not connected; call enqueue() first")
        timeout = timeout_s if timeout_s is not None else self.acquire_timeout_s
        try:
            self.token, self.lease = await self._proto_wait(
                self._reader, self._writer, timeout
            )
        except TimeoutError:
            await self.aclose()
            return False
        except BaseException:
            await self.aclose()
            raise
        self._renew_task = asyncio.create_task(self._renew_loop())
        return True

    async def release(self) -> bool:
        released = False
        try:
            await self._cancel_renew()

            if self._reader and self._writer and self.token:
                await self._proto_release(self._reader, self._writer, self.token)
                released = True
        finally:
            await self.aclose()
        return released

    async def __aenter__(self):
        if not await self.acquire():
            raise TimeoutError(f"timeout acquiring {self.key!r}")
        return self

    async def _renew_loop(self):
        assert self._reader and self._writer and self.token
        interval = max(1.0, self.lease * self.renew_ratio)
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    remaining = await self._proto_renew(
                        self._reader, self._writer, self.token
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.error(
                        "%s lost (renew failed): key=%s token=%s",
                        type(self).__name__,
                        self.key,
                        self.token,
                    )
                    await self.aclose()
                    return
                if remaining > 0:
                    interval = max(1.0, remaining * self.renew_ratio)
        except asyncio.CancelledError:
            return

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._cancel_renew()

            if self._reader and self._writer and self.token:
                with contextlib.suppress(Exception):
                    await self._proto_release(self._reader, self._writer, self.token)
        finally:
            await self.aclose()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self._writer:
            try:
                self._writer.close()
            except Exception:
                pass
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
        self._reader = None
        self._writer = None
        self.token = None


# ===========================================================================
# DistributedLock
# ===========================================================================


@dataclass
class DistributedLock(_AsyncBase):
    async def _proto_acquire(self, reader, writer):
        return await acquire(
            reader, writer, self.key, self.acquire_timeout_s, self.lease_ttl_s
        )

    async def _proto_renew(self, reader, writer, token):
        return await renew(reader, writer, self.key, token, self.lease_ttl_s)

    async def _proto_release(self, reader, writer, token):
        await release(reader, writer, self.key, token)

    async def _proto_enqueue(self, reader, writer):
        return await enqueue(reader, writer, self.key, self.lease_ttl_s)

    async def _proto_wait(self, reader, writer, timeout):
        return await wait(reader, writer, self.key, timeout)


# ===========================================================================
# Semaphore protocol functions
# ===========================================================================


async def sem_acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    # sl\nkey\n"<timeout> <limit> [<lease>]"\n
    arg = f"{acquire_timeout_s} {limit}"
    if lease_ttl_s is not None:
        arg = f"{arg} {lease_ttl_s}"

    writer.write(encode_lines("sl", key, arg))
    await writer.drain()

    resp = await _readline(reader)
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


async def sem_renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    # sn\nkey\n"<token> [<lease>]"\n
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    writer.write(encode_lines("sn", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp != "ok" and not resp.startswith("ok "):
        raise RuntimeError(f"sem_renew failed: {resp!r}")

    parts = resp.split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return -1


async def sem_enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    """
    Two-phase enqueue for semaphore: join FIFO queue, return immediately.
    Returns (status, token, lease) where status is "acquired" or "queued".
    """
    arg = str(limit) if lease_ttl_s is None else f"{limit} {lease_ttl_s}"
    writer.write(encode_lines("se", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp.startswith("acquired "):
        parts = resp.split()
        token = parts[1]
        lease = parse_lease(parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"sem_enqueue failed: {resp!r}")


async def sem_wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    """
    Two-phase wait for semaphore: block until semaphore slot is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    writer.write(encode_lines("sw", key, str(wait_timeout_s)))
    await writer.drain()

    resp = await _readline(reader)
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for semaphore {key!r}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"sem_wait failed: {resp!r}")

    parts = resp.split()
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


async def sem_release(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key: str, token: str
) -> None:
    writer.write(encode_lines("sr", key, token))
    await writer.drain()

    resp = await _readline(reader)
    if resp != "ok":
        raise RuntimeError(f"sem_release failed: {resp!r}")


# ===========================================================================
# DistributedSemaphore
# ===========================================================================


@dataclass
class DistributedSemaphore(_AsyncBase):
    limit: int = 0

    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("limit must be > 0")
        super().__post_init__()

    async def _proto_acquire(self, reader, writer):
        return await sem_acquire(
            reader,
            writer,
            self.key,
            self.acquire_timeout_s,
            self.limit,
            self.lease_ttl_s,
        )

    async def _proto_renew(self, reader, writer, token):
        return await sem_renew(reader, writer, self.key, token, self.lease_ttl_s)

    async def _proto_release(self, reader, writer, token):
        await sem_release(reader, writer, self.key, token)

    async def _proto_enqueue(self, reader, writer):
        return await sem_enqueue(reader, writer, self.key, self.limit, self.lease_ttl_s)

    async def _proto_wait(self, reader, writer, timeout):
        return await sem_wait(reader, writer, self.key, timeout)
