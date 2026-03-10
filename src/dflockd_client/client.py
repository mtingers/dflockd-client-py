import asyncio
import contextlib
import json
import ssl
import warnings
from dataclasses import KW_ONLY, dataclass, field

from ._common import (
    _CONNECT_TIMEOUT_S,
    _MAX_LINE_LEN,
    Signal,
    StatsResult,
    encode_lines,
    log,
    parse_lease,
)
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard


async def _readline(reader: asyncio.StreamReader) -> str:
    try:
        raw = await reader.readline()
    except ValueError as e:
        raise RuntimeError("server response exceeded line length limit") from e
    if raw == b"":
        raise ConnectionError("server closed connection")
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
    *,
    cmd_prefix: str = "",
    limit: int | None = None,
) -> tuple[str, int]:
    parts = [str(acquire_timeout_s)]
    if limit is not None:
        parts.append(str(limit))
    if lease_ttl_s is not None:
        parts.append(str(lease_ttl_s))
    arg = " ".join(parts)

    writer.write(encode_lines(f"{cmd_prefix}l", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    label = f"{'semaphore ' if cmd_prefix else ''}{key!r}"
    if resp == "timeout":
        raise TimeoutError(f"timeout acquiring {label}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"acquire failed: {resp!r}")

    # ok <token> <lease>
    resp_parts = resp.split()
    if len(resp_parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = resp_parts[1]
    lease = parse_lease(resp_parts)
    return token, lease


async def renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
    *,
    cmd_prefix: str = "",
) -> int:
    arg = token if lease_ttl_s is None else f"{token} {lease_ttl_s}"
    writer.write(encode_lines(f"{cmd_prefix}n", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    func = f"{'sem_' if cmd_prefix else ''}renew"
    if resp != "ok" and not resp.startswith("ok "):
        raise RuntimeError(f"{func} failed: {resp!r}")

    parts = resp.split()
    if len(parts) >= 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return -1


async def enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
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
    parts = []
    if limit is not None:
        parts.append(str(limit))
    if lease_ttl_s is not None:
        parts.append(str(lease_ttl_s))
    arg = " ".join(parts)
    writer.write(encode_lines(f"{cmd_prefix}e", key, arg))
    await writer.drain()

    resp = await _readline(reader)
    if resp.startswith("acquired "):
        resp_parts = resp.split()
        if len(resp_parts) < 2:
            raise RuntimeError(f"bad acquired response: {resp!r}")
        token = resp_parts[1]
        lease = parse_lease(resp_parts)
        return ("acquired", token, lease)
    if resp == "queued":
        return ("queued", None, None)
    raise RuntimeError(f"enqueue failed: {resp!r}")


async def wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
    *,
    cmd_prefix: str = "",
) -> tuple[str, int]:
    """
    Two-phase wait: block until lock/semaphore is granted.
    Returns (token, lease). Raises TimeoutError on timeout.
    """
    writer.write(encode_lines(f"{cmd_prefix}w", key, str(wait_timeout_s)))
    await writer.drain()

    resp = await _readline(reader)
    label = f"{'semaphore ' if cmd_prefix else ''}{key!r}"
    if resp == "timeout":
        raise TimeoutError(f"timeout waiting for {label}")
    if not resp.startswith("ok "):
        raise RuntimeError(f"wait failed: {resp!r}")

    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad ok response: {resp!r}")
    token = parts[1]
    lease = parse_lease(parts)
    return token, lease


async def release(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    *,
    cmd_prefix: str = "",
) -> None:
    writer.write(encode_lines(f"{cmd_prefix}r", key, token))
    await writer.drain()

    resp = await _readline(reader)
    func = f"{'sem_' if cmd_prefix else ''}release"
    if resp != "ok":
        raise RuntimeError(f"{func} failed: {resp!r}")


async def stats(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> StatsResult:
    writer.write(encode_lines("stats", "_", "_"))
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
                # Best-effort cleanup: close the transport to release the FD.
                # We can't await wait_closed() here, but transport.close() is
                # synchronous and sufficient to free the underlying socket.
                try:
                    self._writer.close()
                except Exception:
                    pass
        except BaseException:
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
        task = self._renew_task
        if task is not None:
            if task is not asyncio.current_task():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
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
            try:
                self._writer.write(encode_lines("auth", "_", self.auth_token))
                await self._writer.drain()
                resp = await asyncio.wait_for(
                    _readline(self._reader), timeout=self.connect_timeout_s
                )
            except BaseException:
                await self.aclose()
                raise
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
                try:
                    await self._proto_release(self._reader, self._writer, self.token)
                    released = True
                except Exception:
                    log.warning(
                        "%s explicit release failed (lease will expire server-side): key=%s",
                        type(self).__name__,
                        self.key,
                        exc_info=True,
                    )
        finally:
            await self.aclose()
        return released

    async def __aenter__(self):
        if not await self.acquire():
            raise TimeoutError(f"timeout acquiring {self.key!r}")
        return self

    async def _renew_loop(self):
        reader, writer, token = self._reader, self._writer, self.token
        if not (reader and writer and token):
            return
        lease = self.lease if self.lease > 0 else 30
        interval = max(1.0, lease * self.renew_ratio)
        try:
            while True:
                await asyncio.sleep(interval)
                if self._closed or self._writer is not writer or self.token != token:
                    return
                try:
                    remaining = await self._proto_renew(reader, writer, token)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    if (
                        self._closed
                        or self._writer is None
                        or self._writer is not writer
                    ):
                        return
                    log.error(
                        "%s lost (renew failed): key=%s token=%s",
                        type(self).__name__,
                        self.key,
                        self.token,
                    )
                    return
                if remaining > 0:
                    interval = max(1.0, remaining * self.renew_ratio)
        except asyncio.CancelledError:
            return
        finally:
            del reader, writer, token

    async def __aexit__(self, exc_type, exc, tb):
        with contextlib.suppress(Exception):
            await self.release()

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        await self._cancel_renew()
        writer = self._writer
        self._reader = None
        self._writer = None
        self.token = None
        self.lease = 0
        if writer:
            try:
                writer.close()
            except Exception:
                pass
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), timeout=5)


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
# Semaphore protocol wrappers (thin delegates to unified functions above)
# ===========================================================================


async def sem_acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]:
    if limit <= 0:
        raise ValueError("limit must be > 0")
    return await acquire(
        reader,
        writer,
        key,
        acquire_timeout_s,
        lease_ttl_s,
        cmd_prefix="s",
        limit=limit,
    )


async def sem_renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int:
    return await renew(reader, writer, key, token, lease_ttl_s, cmd_prefix="s")


async def sem_enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]:
    if limit <= 0:
        raise ValueError("limit must be > 0")
    return await enqueue(
        reader,
        writer,
        key,
        lease_ttl_s,
        cmd_prefix="s",
        limit=limit,
    )


async def sem_wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]:
    return await wait(reader, writer, key, wait_timeout_s, cmd_prefix="s")


async def sem_release(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, key: str, token: str
) -> None:
    await release(reader, writer, key, token, cmd_prefix="s")


# ===========================================================================
# DistributedSemaphore
# ===========================================================================


@dataclass
class DistributedSemaphore(_AsyncBase):
    limit: int

    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("limit must be > 0")
        super().__post_init__()

    async def _proto_acquire(self, reader, writer):
        return await acquire(
            reader,
            writer,
            self.key,
            self.acquire_timeout_s,
            self.lease_ttl_s,
            cmd_prefix="s",
            limit=self.limit,
        )

    async def _proto_renew(self, reader, writer, token):
        return await renew(
            reader,
            writer,
            self.key,
            token,
            self.lease_ttl_s,
            cmd_prefix="s",
        )

    async def _proto_release(self, reader, writer, token):
        await release(reader, writer, self.key, token, cmd_prefix="s")

    async def _proto_enqueue(self, reader, writer):
        return await enqueue(
            reader,
            writer,
            self.key,
            self.lease_ttl_s,
            cmd_prefix="s",
            limit=self.limit,
        )

    async def _proto_wait(self, reader, writer, timeout):
        return await wait(reader, writer, self.key, timeout, cmd_prefix="s")


# ===========================================================================
# Signal protocol functions
# ===========================================================================


async def sig_emit(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    channel: str,
    payload: str,
) -> int:
    """Emit a signal on a literal channel (no wildcards).

    Returns the number of listeners the signal was delivered to.
    Works on a plain reader/writer pair without a SignalConn.
    """
    if "*" in channel or ">" in channel:
        raise ValueError("channel must not contain wildcards (* or >)")
    writer.write(encode_lines("signal", channel, payload))
    await writer.drain()
    resp = await _readline(reader)
    if not resp.startswith("ok "):
        raise RuntimeError(f"signal failed: {resp!r}")
    parts = resp.split()
    if len(parts) < 2:
        raise RuntimeError(f"bad signal response: {resp!r}")
    try:
        return int(parts[1])
    except ValueError as e:
        raise RuntimeError(f"bad signal response: {resp!r}") from e


# ===========================================================================
# SignalConn (async)
# ===========================================================================


@dataclass
class SignalConn:
    """Async pub/sub signal connection.

    Maintains a background reader that routes push signals to a queue
    while allowing listen/unlisten/emit commands.

    Usage::

        async with SignalConn(server=("127.0.0.1", 6388)) as sc:
            await sc.listen("events.>")
            await sc.emit("events.user.login", "alice")
            async for sig in sc:
                print(sig.channel, sig.payload)
    """

    _: KW_ONLY
    server: tuple[str, int] = ("127.0.0.1", 6388)
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = _CONNECT_TIMEOUT_S

    _reader: asyncio.StreamReader | None = field(default=None, init=False, repr=False)
    _writer: asyncio.StreamWriter | None = field(default=None, init=False, repr=False)
    _read_task: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _sig_queue: asyncio.Queue[Signal | None] = field(
        default_factory=lambda: asyncio.Queue(maxsize=64), init=False, repr=False
    )
    _cmd_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock, init=False, repr=False
    )
    _resp_future: asyncio.Future[str] | None = field(
        default=None, init=False, repr=False
    )
    _closed: bool = field(default=False, init=False, repr=False)

    async def connect(self) -> None:
        """Connect to the server and start the background reader."""
        await self.aclose()
        self._closed = False
        self._sig_queue = asyncio.Queue(maxsize=64)
        host, port = self.server
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(
                host, port, ssl=self.ssl_context, limit=_MAX_LINE_LEN
            ),
            timeout=self.connect_timeout_s,
        )
        if self.auth_token is not None:
            try:
                self._writer.write(encode_lines("auth", "_", self.auth_token))
                await self._writer.drain()
                resp = await asyncio.wait_for(
                    _readline(self._reader), timeout=self.connect_timeout_s
                )
            except BaseException:
                await self.aclose()
                raise
            if resp != "ok":
                await self.aclose()
                raise PermissionError(f"authentication failed: {resp!r}")
        self._read_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                line = await _readline(self._reader)
                if line.startswith("sig "):
                    rest = line[4:]
                    idx = rest.find(" ")
                    if idx < 0:
                        continue
                    sig = Signal(channel=rest[:idx], payload=rest[idx + 1 :])
                    try:
                        self._sig_queue.put_nowait(sig)
                    except asyncio.QueueFull:
                        pass
                else:
                    fut = self._resp_future
                    if fut is not None and not fut.done():
                        fut.set_result(line)
        except (ConnectionError, asyncio.CancelledError, RuntimeError, ValueError):
            pass
        finally:
            fut = self._resp_future
            if fut is not None and not fut.done():
                fut.set_exception(ConnectionError("connection closed"))
            # Ensure the sentinel is delivered even if the queue is full
            # so that ``async for sig in sc:`` terminates cleanly.
            try:
                self._sig_queue.put_nowait(None)
            except asyncio.QueueFull:
                try:
                    self._sig_queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                with contextlib.suppress(asyncio.QueueFull):
                    self._sig_queue.put_nowait(None)

    async def _send_cmd(self, cmd: str, key: str, arg: str) -> str:
        async with self._cmd_lock:
            if self._writer is None:
                raise RuntimeError("not connected; call connect() first")
            loop = asyncio.get_running_loop()
            self._resp_future = loop.create_future()
            self._writer.write(encode_lines(cmd, key, arg))
            await self._writer.drain()
            try:
                return await self._resp_future
            finally:
                self._resp_future = None

    async def listen(self, pattern: str, *, group: str = "") -> None:
        """Subscribe to signals matching *pattern*.

        Supports NATS-style wildcards: ``*`` matches one token,
        ``>`` matches one or more trailing tokens.
        *group* enables queue-group load balancing (round-robin within group).
        """
        resp = await self._send_cmd("listen", pattern, group)
        if resp != "ok":
            raise RuntimeError(f"listen failed: {resp!r}")

    async def unlisten(self, pattern: str, *, group: str = "") -> None:
        """Remove a signal subscription. Pattern and group must match the
        original :meth:`listen` call."""
        resp = await self._send_cmd("unlisten", pattern, group)
        if resp != "ok":
            raise RuntimeError(f"unlisten failed: {resp!r}")

    async def emit(self, channel: str, payload: str) -> int:
        """Publish a signal on a literal channel (no wildcards).

        Returns the number of listeners the signal was delivered to.
        """
        if "*" in channel or ">" in channel:
            raise ValueError("channel must not contain wildcards (* or >)")
        resp = await self._send_cmd("signal", channel, payload)
        if not resp.startswith("ok "):
            raise RuntimeError(f"signal failed: {resp!r}")
        parts = resp.split()
        if len(parts) < 2:
            raise RuntimeError(f"bad signal response: {resp!r}")
        try:
            return int(parts[1])
        except ValueError as e:
            raise RuntimeError(f"bad signal response: {resp!r}") from e

    @property
    def signals(self) -> asyncio.Queue[Signal | None]:
        """Queue of received signals. ``None`` sentinel indicates connection closed."""
        return self._sig_queue

    def __aiter__(self):
        return self._iter_signals()

    async def _iter_signals(self):
        while True:
            item = await self._sig_queue.get()
            if item is None:
                return
            yield item

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.aclose()

    async def aclose(self) -> None:
        """Close the connection and stop the background reader."""
        if self._closed:
            return
        self._closed = True
        if self._read_task is not None:
            self._read_task.cancel()
            with contextlib.suppress(BaseException):
                await self._read_task
            self._read_task = None
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
