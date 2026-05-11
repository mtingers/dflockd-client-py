"""Async transport + low-level protocol functions, with a fake ``AsyncConn``."""

from __future__ import annotations

import asyncio
import contextlib
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

    async def command(
        self, cmd: str, key: str, arg: str, *, read_timeout: float
    ) -> str:
        self.calls.append(_Call(cmd, key, arg, read_timeout))
        return self.responses.pop(0)

    async def close(self) -> None:
        self.closed = True

    def close_nowait(self) -> None:
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

    async def test_maybe_authenticate_none_skips_auth(self):
        conn = _conn()
        assert await da._maybe_authenticate(conn, None) is conn
        assert _calls(conn) == []

    async def test_maybe_authenticate_empty_string_is_explicit_token(self):
        conn = _conn("ok")
        assert await da._maybe_authenticate(conn, "") is conn
        assert _calls(conn)[0].cmd == "auth"


# ---------------------------------------------------------------------------
# AsyncConn read-line and oversized-response handling
# ---------------------------------------------------------------------------


class TestAsyncConnReadLine:
    async def test_strips_crlf(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=_completed(b"ok abc 30\r\n"))
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

    async def test_reader_limit_error_raises_runtime_error(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=_failed(ValueError("too long")))
        writer = MagicMock(spec=asyncio.StreamWriter)
        conn = da.AsyncConn(reader, writer)
        with pytest.raises(RuntimeError, match="line length"):
            await conn._read_line()

    async def test_invalid_utf8_response_raises_runtime_error(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=_completed(b"ok \xc3"))
        writer = MagicMock(spec=asyncio.StreamWriter)
        conn = da.AsyncConn(reader, writer)
        with pytest.raises(RuntimeError, match="not valid UTF-8"):
            await conn._read_line()


class TestAsyncConnCommand:
    async def test_drain_timeout_closes_transport(self):
        async def slow_drain() -> None:
            await asyncio.sleep(0.2)

        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=_completed(b"ok"))
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.drain = slow_drain
        conn = da.AsyncConn(reader, writer)

        with pytest.raises(asyncio.TimeoutError):
            await conn.command("l", "k", "0", read_timeout=0.01)

        writer.close.assert_called_once()

    async def test_timeout_after_write_closes_transport(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=asyncio.Future())
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.drain = MagicMock(return_value=_completed(None))
        conn = da.AsyncConn(reader, writer)

        with pytest.raises(asyncio.TimeoutError):
            await conn.command("l", "k", "0", read_timeout=0.01)

        writer.close.assert_called_once()

    async def test_cancellation_after_write_closes_transport(self):
        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=asyncio.Future())
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.drain = MagicMock(return_value=_completed(None))
        conn = da.AsyncConn(reader, writer)

        task = asyncio.create_task(conn.command("l", "k", "5", read_timeout=30))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        writer.close.assert_called_once()

    async def test_external_cancellation_during_drain_propagates(self):
        drain_started = asyncio.Event()

        async def slow_drain() -> None:
            drain_started.set()
            await asyncio.sleep(10)

        reader = MagicMock(spec=asyncio.StreamReader)
        reader.readline = MagicMock(return_value=asyncio.Future())
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.drain = slow_drain
        conn = da.AsyncConn(reader, writer)

        task = asyncio.create_task(conn.command("l", "k", "5", read_timeout=30))
        await drain_started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        writer.close.assert_called_once()


def _completed(value: object):
    fut: asyncio.Future = asyncio.Future()
    fut.set_result(value)
    return fut


def _failed(exc: BaseException):
    fut: asyncio.Future = asyncio.Future()
    fut.set_exception(exc)
    return fut


# ---------------------------------------------------------------------------
# DistributedLock unit tests (no server)
# ---------------------------------------------------------------------------


class TestAsyncLockConstruction:
    def test_default_servers(self):
        lock = da.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    def test_invalid_key_raises_at_construction(self):
        with pytest.raises(ValueError, match="must not contain whitespace"):
            da.DistributedLock(key="bad key")

    def test_invalid_acquire_timeout_raises_at_construction(self):
        with pytest.raises(ValueError, match=">= 0"):
            da.DistributedLock(key="k", acquire_timeout_s=-1)

    def test_invalid_lease_ttl_raises_at_construction(self):
        with pytest.raises(ValueError, match=">= 1"):
            da.DistributedLock(key="k", lease_ttl_s=0)

    def test_empty_servers(self):
        with pytest.raises(ValueError, match="non-empty"):
            da.DistributedLock(key="k", servers=[])

    def test_invalid_server_shape_raises_at_construction(self):
        with pytest.raises(TypeError, match="\\(host, port\\)"):
            da.DistributedLock(
                key="k",
                servers=("127.0.0.1", 6388),  # type: ignore[arg-type]
            )

    def test_invalid_server_port_raises_at_construction(self):
        with pytest.raises(ValueError, match="server port"):
            da.DistributedLock(key="k", servers=[("127.0.0.1", 70000)])

    def test_invalid_connect_timeout_raises_at_construction(self):
        with pytest.raises(ValueError, match="connect_timeout_s"):
            da.DistributedLock(key="k", connect_timeout_s=0)

    def test_renew_ratio_out_of_range(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            da.DistributedLock(key="k", renew_ratio=1.0)

    def test_auth_token_default(self):
        assert da.DistributedLock(key="k").auth_token is None

    def test_ssl_context_default(self):
        assert da.DistributedLock(key="k").ssl_context is None

    def test_negative_shard_index_raises_instead_of_routing_last_server(self):
        def bad_shard(_: str, __: int) -> int:
            return -1

        lock = da.DistributedLock(
            key="k",
            servers=[("a", 1), ("b", 2)],
            sharding_strategy=bad_shard,
        )
        with pytest.raises(IndexError, match="returned index -1"):
            lock._pick_server()


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
        lock._conn = fake
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
            lock._conn = None
        rw = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(rw) == 1
        fake.close_nowait.assert_called_once()


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

    async def test_renew_failure_drops_broken_connection(self):
        class Lock(da.DistributedLock):
            async def _proto_renew(self, conn: da.AsyncConn, token: str) -> int:
                raise RuntimeError("boom")

        conn = _conn()
        lock = Lock(key="k")
        lock._conn = conn
        lock.token = "tok"
        lock.lease = 10

        assert await lock._renew_tick() is None
        assert lock._conn is None
        assert lock.token is None
        assert lock.lease == 0
        assert cast(FakeConn, conn).closed is True

    def test_renew_failure_log_redacts_token(self, caplog):
        lock = da.DistributedLock(key="k")
        lock.token = "0123456789abcdef0123456789abcdef"

        with caplog.at_level("ERROR", logger="dflockd_client"):
            lock._log_renew_failure()

        assert "01234567..." in caplog.text
        assert lock.token not in caplog.text


class TestAsyncLifecycleCancellation:
    async def test_release_cancellation_still_clears_state(self):
        class Lock(da.DistributedLock):
            async def _proto_release(self, conn: da.AsyncConn, token: str) -> None:
                raise asyncio.CancelledError

        conn = _conn()
        lock = Lock(key="k")
        lock._conn = conn
        lock.token = "tok"
        lock.lease = 10

        with pytest.raises(asyncio.CancelledError):
            await lock.release()

        assert lock._conn is None
        assert lock.token is None
        assert lock.lease == 0
        assert cast(FakeConn, conn).closed is True

    async def test_aclose_cancellation_still_clears_state(self):
        class CancelCloseConn(FakeConn):
            async def close(self) -> None:
                self.closed = True
                raise asyncio.CancelledError

        conn = cast(da.AsyncConn, CancelCloseConn())
        lock = da.DistributedLock(key="k")
        lock._conn = conn
        lock.token = "tok"
        lock.lease = 10

        with pytest.raises(asyncio.CancelledError):
            await lock.aclose()

        assert lock._conn is None
        assert lock.token is None
        assert lock.lease == 0
        assert cast(FakeConn, conn).closed is True

    async def test_release_waits_for_in_flight_renew(self):
        renew_started = asyncio.Event()
        renew_continue = asyncio.Event()
        events: list[str] = []

        class Lock(da.DistributedLock):
            async def _proto_renew(self, conn: da.AsyncConn, token: str) -> int:
                events.append("renew-start")
                renew_started.set()
                await renew_continue.wait()
                events.append("renew-end")
                return 10

            async def _proto_release(self, conn: da.AsyncConn, token: str) -> None:
                events.append("release")

        conn = _conn()
        lock = Lock(key="k")
        lock._conn = conn
        lock.token = "tok"
        lock.lease = 10

        async def run_renew_tick() -> None:
            await lock._renew_tick()

        renew_task = asyncio.create_task(run_renew_tick())
        lock._renew_task = renew_task
        await renew_started.wait()

        release_task = asyncio.create_task(lock.release())
        await asyncio.sleep(0.05)
        assert release_task.done() is False
        assert renew_task.done() is False

        renew_continue.set()
        assert await release_task is True
        assert events == ["renew-start", "renew-end", "release"]

    async def test_aclose_propagates_cancellation_while_stopping_renew(self):
        cancel_seen = asyncio.Event()
        finish_cancel = asyncio.Event()

        async def slow_cancel() -> None:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancel_seen.set()
                await finish_cancel.wait()
                raise

        lock = da.DistributedLock(key="k")
        renew_task = asyncio.create_task(slow_cancel())
        lock._renew_task = renew_task

        close_task = asyncio.create_task(lock.aclose())
        await cancel_seen.wait()
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        finish_cancel.set()
        with contextlib.suppress(asyncio.CancelledError):
            await renew_task
