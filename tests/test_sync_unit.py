"""Sync transport + low-level protocol functions, with a fake ``SyncConn``.

Each test wires up a minimal fake that records every ``command`` call and
returns a canned response. This way we can lock down the wire format
without hitting a real server.
"""

from __future__ import annotations

import socket
import warnings
from dataclasses import dataclass, field
from typing import cast
from unittest.mock import MagicMock

import pytest

import dflockd_client._sync as ds
from dflockd_client.errors import (
    AlreadyQueuedError,
    DflockdTimeoutError,
    LeaseExpiredError,
    MaxLocksError,
    NotQueuedError,
)


# ---------------------------------------------------------------------------
# Fake conn
# ---------------------------------------------------------------------------


@dataclass
class _Call:
    cmd: str
    key: str
    arg: str
    read_timeout: float | None


@dataclass
class FakeConn:
    """Records every ``command`` invocation and returns canned responses."""

    responses: list[str] = field(default_factory=list)
    calls: list[_Call] = field(default_factory=list)
    closed: bool = False

    def command(self, cmd: str, key: str, arg: str, *, read_timeout: float | None) -> str:
        self.calls.append(_Call(cmd, key, arg, read_timeout))
        return self.responses.pop(0)

    def shutdown_read(self) -> None: ...

    def close(self) -> None:
        self.closed = True


def _conn(*responses: str) -> ds.SyncConn:
    """Construct a typed FakeConn. Cast keeps the structural-typing tests
    type-checked without making ``SyncConn`` an explicit Protocol in the
    library (which would only be useful for tests)."""
    return cast(ds.SyncConn, FakeConn(responses=list(responses)))


def _calls(conn: ds.SyncConn) -> list[_Call]:
    return cast(FakeConn, conn).calls


# ---------------------------------------------------------------------------
# Low-level acquire
# ---------------------------------------------------------------------------


class TestSyncAcquire:
    def test_grant(self):
        conn = _conn("ok tok 33")
        token, lease = ds.acquire(conn, "k", 5)
        assert (token, lease) == ("tok", 33)
        assert _calls(conn)[0].cmd == "l"
        assert _calls(conn)[0].key == "k"
        assert _calls(conn)[0].arg == "5"

    def test_grant_with_lease_ttl(self):
        conn = _conn("ok tok 30")
        ds.acquire(conn, "k", 5, lease_ttl_s=30)
        assert _calls(conn)[0].arg == "5 30"

    def test_timeout_raises(self):
        with pytest.raises(DflockdTimeoutError):
            ds.acquire(_conn("timeout"), "k", 5)

    def test_max_locks_raises(self):
        with pytest.raises(MaxLocksError):
            ds.acquire(_conn("error_max_locks"), "k", 5)

    def test_lock_with_limit_raises_value_error(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            ds.acquire(_conn(), "k", 5, limit=2)

    def test_semaphore_command(self):
        conn = _conn("ok tok 30")
        ds.acquire(conn, "k", 5, prefix="s", limit=3)
        assert _calls(conn)[0].cmd == "sl"
        assert _calls(conn)[0].arg == "5 3"

    def test_semaphore_without_limit_raises(self):
        with pytest.raises(ValueError, match="limit is required"):
            ds.acquire(_conn(), "k", 5, prefix="s")


class TestSyncRelease:
    def test_ok(self):
        conn = _conn("ok")
        ds.release(conn, "k", "tok")
        assert _calls(conn)[0].cmd == "r"
        assert _calls(conn)[0].key == "k"
        assert _calls(conn)[0].arg == "tok"

    def test_semaphore(self):
        conn = _conn("ok")
        ds.release(conn, "k", "tok", prefix="s")
        assert _calls(conn)[0].cmd == "sr"

    def test_error(self):
        with pytest.raises(Exception, match="release failed"):
            ds.release(_conn("error"), "k", "tok")

    def test_empty_token_rejected(self):
        with pytest.raises(ValueError):
            ds.release(_conn(), "k", "")


class TestSyncRenew:
    def test_returns_remaining(self):
        conn = _conn("ok 18")
        assert ds.renew(conn, "k", "tok") == 18
        assert _calls(conn)[0].cmd == "n"
        assert _calls(conn)[0].arg == "tok"

    def test_with_lease_ttl(self):
        conn = _conn("ok 30")
        ds.renew(conn, "k", "tok", lease_ttl_s=30)
        assert _calls(conn)[0].arg == "tok 30"

    def test_lease_expired(self):
        with pytest.raises(LeaseExpiredError):
            ds.renew(_conn("error_lease_expired"), "k", "tok")

    def test_semaphore(self):
        conn = _conn("ok 5")
        ds.renew(conn, "k", "tok", prefix="s")
        assert _calls(conn)[0].cmd == "sn"


class TestSyncEnqueue:
    def test_queued(self):
        conn = _conn("queued")
        assert ds.enqueue(conn, "k") == ("queued", None, None)
        assert _calls(conn)[0].cmd == "e"
        assert _calls(conn)[0].arg == ""

    def test_acquired_fast_path(self):
        conn = _conn("acquired tok 30")
        assert ds.enqueue(conn, "k") == ("acquired", "tok", 30)

    def test_already_enqueued(self):
        with pytest.raises(AlreadyQueuedError):
            ds.enqueue(_conn("error_already_enqueued"), "k")

    def test_semaphore_includes_limit(self):
        conn = _conn("queued")
        ds.enqueue(conn, "k", prefix="s", limit=4)
        assert _calls(conn)[0].cmd == "se"
        assert _calls(conn)[0].arg == "4"

    def test_semaphore_with_lease(self):
        conn = _conn("queued")
        ds.enqueue(conn, "k", lease_ttl_s=10, prefix="s", limit=4)
        assert _calls(conn)[0].arg == "4 10"


class TestSyncWait:
    def test_grant(self):
        conn = _conn("ok tok 30")
        token, lease = ds.wait(conn, "k", 5)
        assert (token, lease) == ("tok", 30)
        assert _calls(conn)[0].cmd == "w"
        assert _calls(conn)[0].arg == "5"

    def test_timeout(self):
        with pytest.raises(DflockdTimeoutError):
            ds.wait(_conn("timeout"), "k", 1)

    def test_not_enqueued(self):
        with pytest.raises(NotQueuedError):
            ds.wait(_conn("error_not_enqueued"), "k", 1)

    def test_long_poll_uses_extended_read_timeout(self):
        """Wait sets the socket read timeout to ``timeout + slack`` so a
        slow-but-alive server can't wedge the caller past the user's
        intent — but a healthy long poll runs to completion."""
        conn = _conn("timeout")
        try:
            ds.wait(conn, "k", 5)
        except DflockdTimeoutError:
            pass
        assert _calls(conn)[0].read_timeout == 5 + ds._IO_SLACK_S


class TestSyncSemConvenience:
    def test_sem_acquire_uses_prefix(self):
        conn = _conn("ok tok 30")
        ds.sem_acquire(conn, "k", 5, 2)
        assert _calls(conn)[0].cmd == "sl"

    def test_sem_release_uses_prefix(self):
        ds.sem_release(_conn("ok"), "k", "tok")

    def test_sem_renew_uses_prefix(self):
        conn = _conn("ok 5")
        ds.sem_renew(conn, "k", "tok")
        assert _calls(conn)[0].cmd == "sn"

    def test_sem_enqueue_uses_prefix(self):
        conn = _conn("queued")
        ds.sem_enqueue(conn, "k", 3)
        assert _calls(conn)[0].cmd == "se"

    def test_sem_wait_uses_prefix(self):
        conn = _conn("ok t 5")
        ds.sem_wait(conn, "k", 1)
        assert _calls(conn)[0].cmd == "sw"


class TestSyncStats:
    def test_decodes_json(self):
        conn = _conn(
            'ok {"connections":1,"locks":[],"semaphores":[],'
            '"idle_locks":[],"idle_semaphores":[]}'
        )
        result = ds.stats(conn)
        assert result["connections"] == 1


class TestSyncAuthenticate:
    def test_ok(self):
        ds.authenticate(_conn("ok"), "secret")

    def test_rejects_newline_in_token(self):
        with pytest.raises(ValueError):
            ds.authenticate(_conn(), "bad\ntoken")

    def test_failed_raises_permission_error(self):
        with pytest.raises(PermissionError):
            ds.authenticate(_conn("error_auth"), "secret")


# ---------------------------------------------------------------------------
# renew_interval
# ---------------------------------------------------------------------------


class TestRenewInterval:
    def test_typical(self):
        assert ds.renew_interval(60, 0.5) == 30.0

    def test_floor_one_second(self):
        assert ds.renew_interval(1, 0.1) == 1.0

    def test_zero_lease_uses_fallback(self):
        assert ds.renew_interval(0, 0.5) == ds._DEFAULT_LEASE_FALLBACK_S * 0.5


# ---------------------------------------------------------------------------
# DistributedLock unit tests (no server)
# ---------------------------------------------------------------------------


class TestSyncLockConstruction:
    def test_default_servers(self):
        lock = ds.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]
        assert lock.token is None

    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            ds.DistributedLock(key="k", servers=[])

    def test_renew_ratio_out_of_range(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            ds.DistributedLock(key="k", renew_ratio=0)

    def test_auth_token_default_none(self):
        assert ds.DistributedLock(key="k").auth_token is None

    def test_ssl_context_default_none(self):
        assert ds.DistributedLock(key="k").ssl_context is None


class TestSyncSemaphoreConstruction:
    def test_requires_positive_limit(self):
        with pytest.raises(ValueError, match="limit must be > 0"):
            ds.DistributedSemaphore(key="k", limit=0)

    def test_constructs(self):
        sem = ds.DistributedSemaphore(key="k", limit=3)
        assert sem.limit == 3


class TestSyncDelLeakWarning:
    """A lock garbage-collected without ``release()``/``close()`` should
    surface a ``ResourceWarning`` and best-effort close the socket FD."""

    def test_warns_when_conn_held(self):
        lock = ds.DistributedLock(key="k", servers=[("127.0.0.1", 9999)])
        fake = MagicMock(spec=ds.SyncConn)
        lock._conn = fake
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
            lock._conn = None  # prevent duplicate warning at teardown
        rw = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(rw) == 1
        assert "garbage collected" in str(rw[0].message)
        fake.close.assert_called_once()

    def test_no_warning_when_already_closed(self):
        lock = ds.DistributedLock(key="k", servers=[("127.0.0.1", 9999)])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
        assert len(w) == 0


class TestSyncRenewLoopUpdatesLease:
    """A successful renew tick must update ``self.lease`` to the server's
    reported remaining seconds — otherwise the published lease drifts
    further out of date with each renew."""

    def test_update_lease_only_when_held(self):
        lock = ds.DistributedLock(key="k")
        lock.token = "tok"
        lock._update_lease(42)
        assert lock.lease == 42

    def test_no_update_when_no_token(self):
        lock = ds.DistributedLock(key="k")
        lock.token = None
        lock._update_lease(42)
        assert lock.lease == 0

    def test_no_update_after_close(self):
        lock = ds.DistributedLock(key="k")
        lock.token = "tok"
        lock._closed = True
        lock._update_lease(42)
        assert lock.lease == 0

    def test_no_update_with_zero_remaining(self):
        lock = ds.DistributedLock(key="k")
        lock.token = "tok"
        lock._update_lease(0)
        assert lock.lease == 0


# ---------------------------------------------------------------------------
# SyncConn read-line behaviour
# ---------------------------------------------------------------------------


class TestSyncConnReadLine:
    def test_strips_crlf(self):
        sock = MagicMock(spec=socket.socket)
        conn = ds.SyncConn(sock)
        conn._rfile = MagicMock()
        conn._rfile.readline.return_value = "ok abc 30\r\n"
        assert conn._read_line() == "ok abc 30"

    def test_eof_raises_connection_error(self):
        sock = MagicMock(spec=socket.socket)
        conn = ds.SyncConn(sock)
        conn._rfile = MagicMock()
        conn._rfile.readline.return_value = ""
        with pytest.raises(ConnectionError, match="server closed connection"):
            conn._read_line()

    def test_oversized_response_raises(self):
        from dflockd_client import _protocol as proto
        sock = MagicMock(spec=socket.socket)
        conn = ds.SyncConn(sock)
        conn._rfile = MagicMock()
        conn._rfile.readline.return_value = "x" * (proto.MAX_RESPONSE_LINE_BYTES + 1)
        with pytest.raises(RuntimeError, match="too large"):
            conn._read_line()
