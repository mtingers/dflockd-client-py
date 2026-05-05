"""Async transport + low-level protocol functions, with a fake ``AsyncConn``."""

from __future__ import annotations

import asyncio
import warnings
from dataclasses import dataclass, field
from typing import cast
from unittest.mock import MagicMock

import pytest

import dflockd_client._async as da
from dflockd_client.errors import (
    AlreadyQueuedError,
    DflockdTimeoutError,
    LeaseExpiredError,
    MaxLocksError,
    NotQueuedError,
)


@dataclass
class _Call:
    cmd: str
    key: str
    arg: str
    read_timeout: float


@dataclass
class FakeConn:
    responses: list[str] = field(default_factory=list)
    calls: list[_Call] = field(default_factory=list)
    closed: bool = False

    async def command(self, cmd: str, key: str, arg: str, *, read_timeout: float) -> str:
        self.calls.append(_Call(cmd, key, arg, read_timeout))
        return self.responses.pop(0)

    async def close(self) -> None:
        self.closed = True


def _conn(*responses: str) -> da.AsyncConn:
    return cast(da.AsyncConn, FakeConn(responses=list(responses)))


def _calls(conn: da.AsyncConn) -> list[_Call]:
    return cast(FakeConn, conn).calls


# ---------------------------------------------------------------------------
# Low-level acquire
# ---------------------------------------------------------------------------


class TestAsyncAcquire:
    async def test_grant(self):
        conn = _conn("ok tok 33")
        token, lease = await da.acquire(conn, "k", 5)
        assert (token, lease) == ("tok", 33)
        assert _calls(conn)[0].cmd == "l"
        assert _calls(conn)[0].arg == "5"

    async def test_with_lease_ttl(self):
        conn = _conn("ok t 30")
        await da.acquire(conn, "k", 5, lease_ttl_s=30)
        assert _calls(conn)[0].arg == "5 30"

    async def test_timeout_raises(self):
        with pytest.raises(DflockdTimeoutError):
            await da.acquire(_conn("timeout"), "k", 5)

    async def test_max_locks_raises(self):
        with pytest.raises(MaxLocksError):
            await da.acquire(_conn("error_max_locks"), "k", 5)

    async def test_lock_with_limit_raises(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            await da.acquire(_conn(), "k", 5, limit=2)

    async def test_semaphore(self):
        conn = _conn("ok t 30")
        await da.acquire(conn, "k", 5, prefix="s", limit=3)
        assert _calls(conn)[0].cmd == "sl"
        assert _calls(conn)[0].arg == "5 3"

    async def test_semaphore_without_limit_raises(self):
        with pytest.raises(ValueError, match="limit is required"):
            await da.acquire(_conn(), "k", 5, prefix="s")


class TestAsyncRelease:
    async def test_ok(self):
        conn = _conn("ok")
        await da.release(conn, "k", "tok")
        assert _calls(conn)[0].cmd == "r"

    async def test_semaphore(self):
        conn = _conn("ok")
        await da.release(conn, "k", "tok", prefix="s")
        assert _calls(conn)[0].cmd == "sr"

    async def test_error(self):
        with pytest.raises(Exception, match="release failed"):
            await da.release(_conn("error"), "k", "tok")

    async def test_empty_token_rejected(self):
        with pytest.raises(ValueError):
            await da.release(_conn(), "k", "")


class TestAsyncRenew:
    async def test_returns_remaining(self):
        conn = _conn("ok 18")
        assert await da.renew(conn, "k", "tok") == 18
        assert _calls(conn)[0].cmd == "n"
        assert _calls(conn)[0].arg == "tok"

    async def test_with_lease_ttl(self):
        conn = _conn("ok 30")
        await da.renew(conn, "k", "tok", lease_ttl_s=30)
        assert _calls(conn)[0].arg == "tok 30"

    async def test_lease_expired(self):
        with pytest.raises(LeaseExpiredError):
            await da.renew(_conn("error_lease_expired"), "k", "tok")

    async def test_semaphore(self):
        conn = _conn("ok 5")
        await da.renew(conn, "k", "tok", prefix="s")
        assert _calls(conn)[0].cmd == "sn"


class TestAsyncEnqueue:
    async def test_queued(self):
        conn = _conn("queued")
        assert await da.enqueue(conn, "k") == ("queued", None, None)
        assert _calls(conn)[0].cmd == "e"
        assert _calls(conn)[0].arg == ""

    async def test_acquired_fast_path(self):
        conn = _conn("acquired tok 30")
        assert await da.enqueue(conn, "k") == ("acquired", "tok", 30)

    async def test_already_enqueued(self):
        with pytest.raises(AlreadyQueuedError):
            await da.enqueue(_conn("error_already_enqueued"), "k")

    async def test_semaphore_includes_limit(self):
        conn = _conn("queued")
        await da.enqueue(conn, "k", prefix="s", limit=4)
        assert _calls(conn)[0].cmd == "se"
        assert _calls(conn)[0].arg == "4"


class TestAsyncWait:
    async def test_grant(self):
        conn = _conn("ok tok 30")
        token, lease = await da.wait(conn, "k", 5)
        assert (token, lease) == ("tok", 30)
        assert _calls(conn)[0].cmd == "w"
        assert _calls(conn)[0].arg == "5"

    async def test_timeout(self):
        with pytest.raises(DflockdTimeoutError):
            await da.wait(_conn("timeout"), "k", 1)

    async def test_not_enqueued(self):
        with pytest.raises(NotQueuedError):
            await da.wait(_conn("error_not_enqueued"), "k", 1)

    async def test_long_poll_uses_extended_read_timeout(self):
        conn = _conn("timeout")
        try:
            await da.wait(conn, "k", 5)
        except DflockdTimeoutError:
            pass
        assert _calls(conn)[0].read_timeout == 5 + da._IO_SLACK_S


class TestAsyncSemConvenience:
    async def test_sem_acquire(self):
        conn = _conn("ok t 30")
        await da.sem_acquire(conn, "k", 5, 2)
        assert _calls(conn)[0].cmd == "sl"

    async def test_sem_release(self):
        await da.sem_release(_conn("ok"), "k", "tok")

    async def test_sem_renew(self):
        conn = _conn("ok 5")
        await da.sem_renew(conn, "k", "tok")
        assert _calls(conn)[0].cmd == "sn"

    async def test_sem_enqueue(self):
        conn = _conn("queued")
        await da.sem_enqueue(conn, "k", 3)
        assert _calls(conn)[0].cmd == "se"

    async def test_sem_wait(self):
        conn = _conn("ok t 5")
        await da.sem_wait(conn, "k", 1)
        assert _calls(conn)[0].cmd == "sw"


class TestAsyncStats:
    async def test_decodes_json(self):
        conn = _conn(
            'ok {"connections":1,"locks":[],"semaphores":[],'
            '"idle_locks":[],"idle_semaphores":[]}'
        )
        result = await da.stats(conn)
        assert result["connections"] == 1


class TestAsyncAuthenticate:
    async def test_ok(self):
        await da.authenticate(_conn("ok"), "secret")

    async def test_failed_raises_permission_error(self):
        with pytest.raises(PermissionError):
            await da.authenticate(_conn("error_auth"), "secret")


# ---------------------------------------------------------------------------
# AsyncConn read-line and oversized-response handling
# ---------------------------------------------------------------------------


class TestAsyncConnReadLine:
    async def test_strips_crlf(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(
            return_value=_completed(b"ok abc 30\r\n")
        )
        writer = MagicMock(spec=asyncio.StreamWriter)
        conn = da.AsyncConn(reader, writer)
        assert await conn._read_line() == "ok abc 30"

    async def test_eof_raises(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=_completed(b""))
        writer = MagicMock(spec=asyncio.StreamWriter)
        conn = da.AsyncConn(reader, writer)
        with pytest.raises(ConnectionError):
            await conn._read_line()


def _completed(value: bytes):
    fut: asyncio.Future[bytes] = asyncio.Future()
    fut.set_result(value)
    return fut


# ---------------------------------------------------------------------------
# DistributedLock unit tests (no server)
# ---------------------------------------------------------------------------


class TestAsyncLockConstruction:
    def test_default_servers(self):
        lock = da.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    def test_empty_servers(self):
        with pytest.raises(ValueError, match="non-empty"):
            da.DistributedLock(key="k", servers=[])

    def test_renew_ratio_out_of_range(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            da.DistributedLock(key="k", renew_ratio=1.0)

    def test_auth_token_default(self):
        assert da.DistributedLock(key="k").auth_token is None

    def test_ssl_context_default(self):
        assert da.DistributedLock(key="k").ssl_context is None


class TestAsyncSemaphoreConstruction:
    def test_requires_positive_limit(self):
        with pytest.raises(ValueError, match="limit must be > 0"):
            da.DistributedSemaphore(key="k", limit=0)

    def test_constructs(self):
        sem = da.DistributedSemaphore(key="k", limit=3)
        assert sem.limit == 3


class TestAsyncDelLeakWarning:
    def test_warns_when_conn_held(self):
        lock = da.DistributedLock(key="k", servers=[("127.0.0.1", 9999)])
        fake = MagicMock(spec=da.AsyncConn)
        fake._writer = MagicMock()
        lock._conn = fake
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
            lock._conn = None
        rw = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(rw) == 1
        fake._writer.close.assert_called_once()


class TestAsyncRenewLoopUpdatesLease:
    def test_update_lease_only_when_held(self):
        lock = da.DistributedLock(key="k")
        lock.token = "tok"
        lock._update_lease(42)
        assert lock.lease == 42

    def test_no_update_when_no_token(self):
        lock = da.DistributedLock(key="k")
        lock.token = None
        lock._update_lease(42)
        assert lock.lease == 0

    def test_no_update_after_close(self):
        lock = da.DistributedLock(key="k")
        lock.token = "tok"
        lock._closed = True
        lock._update_lease(42)
        assert lock.lease == 0
