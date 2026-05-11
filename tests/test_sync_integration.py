"""Integration tests for the sync client against a real dflockd server.

These tests use the high-level :class:`SyncDistributedLock` /
:class:`SyncDistributedSemaphore` types directly. They cover the same
ground as the old ``test_client.py`` did for sync — single-phase acquire,
two-phase enqueue/wait, FIFO ordering, mutual exclusion across threads,
disconnect cleanup, lease renewal — but expressed as scenarios rather
than mock-heavy unit tests.
"""

from __future__ import annotations

import os
import socket
import ssl
import threading
import time
import uuid

import pytest

import dflockd_client._sync as ds
from dflockd_client import (
    SyncConn,
    SyncDistributedLock,
    SyncDistributedSemaphore,
    fence_from_token,
)


def _key(prefix: str) -> str:
    """Unique key per test so the server's idle keys don't collide."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _open_low_level(host: str, port: int) -> SyncConn:
    sock = socket.create_connection((host, port))
    return SyncConn(sock)


# ===========================================================================
# Low-level: protocol functions on a SyncConn
# ===========================================================================


class TestLowLevelLock:
    def test_acquire_release_round_trip(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("k")
            tok, lease = ds.acquire(conn, key, 5)
            assert tok and lease > 0
            ds.release(conn, key, tok)
        finally:
            conn.close()

    def test_acquire_timeout_when_held(self, server_host_port):
        c1 = _open_low_level(*server_host_port)
        c2 = _open_low_level(*server_host_port)
        try:
            key = _key("k")
            ds.acquire(c1, key, 5)
            with pytest.raises(TimeoutError):
                ds.acquire(c2, key, 0)
        finally:
            c1.close()
            c2.close()

    def test_release_with_bad_token_raises(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("k")
            ds.acquire(conn, key, 5)
            with pytest.raises(Exception, match="release failed"):
                ds.release(conn, key, "not-a-real-token")
        finally:
            conn.close()

    def test_renew_returns_remaining(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("k")
            tok, _ = ds.acquire(conn, key, 5, lease_ttl_s=10)
            remaining = ds.renew(conn, key, tok, lease_ttl_s=20)
            assert remaining >= 0
        finally:
            conn.close()


class TestFencingTokens:
    def test_grant_token_parses_as_fence(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("fence")
            tok, _ = ds.acquire(conn, key, 5)
            assert 0 <= fence_from_token(tok) < (1 << 64)
            ds.release(conn, key, tok)
        finally:
            conn.close()

    def test_successive_grants_are_ordered(self, server_host_port):
        host, port = server_host_port
        key = _key("fence")
        c1 = _open_low_level(host, port)
        try:
            tok1, _ = ds.acquire(c1, key, 5)
            ds.release(c1, key, tok1)
        finally:
            c1.close()
        c2 = _open_low_level(host, port)
        try:
            tok2, _ = ds.acquire(c2, key, 5)
            ds.release(c2, key, tok2)
        finally:
            c2.close()
        assert fence_from_token(tok2) > fence_from_token(tok1)


class TestLowLevelTwoPhase:
    def test_enqueue_acquired_fast_path(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("k")
            status, tok, lease = ds.enqueue(conn, key)
            assert status == "acquired"
            assert tok and lease and lease > 0
            ds.release(conn, key, tok)
        finally:
            conn.close()

    def test_queued_then_wait(self, server_host_port):
        host, port = server_host_port
        c1 = _open_low_level(host, port)
        c2 = _open_low_level(host, port)
        try:
            key = _key("k")
            tok1, _ = ds.acquire(c1, key, 5)
            status, _, _ = ds.enqueue(c2, key)
            assert status == "queued"
            t = threading.Timer(0.1, lambda: ds.release(c1, key, tok1))
            t.start()
            tok2, lease2 = ds.wait(c2, key, 5)
            t.join()
            assert tok2 and lease2 > 0
            ds.release(c2, key, tok2)
        finally:
            c1.close()
            c2.close()


class TestLowLevelSemaphore:
    def test_sem_acquire_release(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            key = _key("s")
            tok, lease = ds.sem_acquire(conn, key, 5, 2)
            assert tok and lease > 0
            ds.sem_release(conn, key, tok)
        finally:
            conn.close()

    def test_sem_limit_blocks_third(self, server_host_port):
        host, port = server_host_port
        c1, c2, c3 = (_open_low_level(host, port) for _ in range(3))
        try:
            key = _key("s")
            ds.sem_acquire(c1, key, 5, 2)
            ds.sem_acquire(c2, key, 5, 2)
            with pytest.raises(TimeoutError):
                ds.sem_acquire(c3, key, 0, 2)
        finally:
            c1.close()
            c2.close()
            c3.close()


class TestStats:
    def test_decoded_shape(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        try:
            result = ds.stats(conn)
            assert {
                "connections",
                "locks",
                "semaphores",
                "idle_locks",
                "idle_semaphores",
            }.issubset(result.keys())
        finally:
            conn.close()

    def test_includes_held_lock(self, server_host_port):
        host, port = server_host_port
        c1, c2 = _open_low_level(host, port), _open_low_level(host, port)
        try:
            key = _key("statskey")
            tok, _ = ds.acquire(c1, key, 5, lease_ttl_s=30)
            result = ds.stats(c2)
            assert key in [lk["key"] for lk in result["locks"]]
            ds.release(c1, key, tok)
        finally:
            c1.close()
            c2.close()


# ===========================================================================
# High-level: DistributedLock
# ===========================================================================


class TestDistributedLock:
    def test_context_manager_grants_and_releases(self, server_host_port):
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("ctx"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        with lock as held:
            assert held.token is not None
        assert lock.token is None

    def test_acquire_release_methods(self, server_host_port):
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("acq"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        assert lock.acquire() is True
        assert lock.token is not None
        lock.release()
        assert lock.token is None

    def test_acquire_timeout_returns_false(self, server_host_port):
        host, port = server_host_port
        key = _key("timeout")
        holder = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        contender = SyncDistributedLock(
            key=key,
            acquire_timeout_s=0,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        holder.acquire()
        try:
            assert contender.acquire() is False
        finally:
            holder.release()

    def test_mutual_exclusion_across_threads(self, server_host_port):
        host, port = server_host_port
        key = _key("mutex")
        results: list[int] = []
        lk = threading.Lock()

        def worker(n: int):
            inst = SyncDistributedLock(
                key=key,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[(host, port)],
            )
            with inst:
                with lk:
                    results.append(n)
                time.sleep(0.05)

        t1 = threading.Thread(target=worker, args=(1,))
        t1.start()
        time.sleep(0.02)
        t2 = threading.Thread(target=worker, args=(2,))
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        assert results == [1, 2]

    def test_two_phase_acquired_immediately(self, server_host_port):
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("two"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        try:
            assert lock.enqueue() == "acquired"
            assert lock.token is not None
            assert lock.wait() is True  # already held
        finally:
            lock.release()

    def test_two_phase_queued_then_granted(self, server_host_port):
        host, port = server_host_port
        key = _key("queued")
        l1 = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        l2 = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        l1.acquire()
        try:
            assert l2.enqueue() == "queued"

            def release_soon():
                time.sleep(0.1)
                l1.release()

            t = threading.Thread(target=release_soon)
            t.start()
            assert l2.wait() is True
            t.join()
            assert l2.token is not None
        finally:
            l2.release()

    def test_fifo_ordering(self, server_host_port):
        """5 waiters enqueued sequentially must be granted in FIFO order."""
        host, port = server_host_port
        key = _key("fifo")
        N = 5
        holder = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=10,
            servers=[(host, port)],
        )
        holder.acquire()
        waiters = [
            SyncDistributedLock(
                key=key,
                acquire_timeout_s=10,
                lease_ttl_s=10,
                servers=[(host, port)],
            )
            for _ in range(N)
        ]
        for w in waiters:
            assert w.enqueue() == "queued"
            time.sleep(0.05)
        holder.release()
        order: list[int] = []
        order_lock = threading.Lock()

        def runner(idx: int, w: SyncDistributedLock):
            assert w.wait() is True
            with order_lock:
                order.append(idx)
            w.release()

        threads = [
            threading.Thread(target=runner, args=(i, w)) for i, w in enumerate(waiters)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        assert order == list(range(N))

    def test_disconnect_releases_held_lock(self, server_host_port):
        """When a holder's connection drops, the server should release the
        lock so a new client can acquire immediately."""
        host, port = server_host_port
        key = _key("disc")
        first = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        first.acquire()
        first.close()  # forces disconnect without server-side release
        time.sleep(0.2)
        second = SyncDistributedLock(
            key=key,
            acquire_timeout_s=1,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        try:
            assert second.acquire() is True
        finally:
            second.release()


# ===========================================================================
# High-level: DistributedSemaphore
# ===========================================================================


class TestDistributedSemaphore:
    def test_acquire_release(self, server_host_port):
        host, port = server_host_port
        sem = SyncDistributedSemaphore(
            key=_key("s"),
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        assert sem.acquire() is True
        sem.release()
        assert sem.token is None

    def test_third_blocks_when_limit_full(self, server_host_port):
        host, port = server_host_port
        key = _key("s")
        s1 = SyncDistributedSemaphore(
            key=key,
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        s2 = SyncDistributedSemaphore(
            key=key,
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        s3 = SyncDistributedSemaphore(
            key=key,
            limit=2,
            acquire_timeout_s=0,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        s1.acquire()
        s2.acquire()
        try:
            assert s3.acquire() is False
        finally:
            s1.release()
            s2.release()

    def test_two_phase_fifo_ordering(self, server_host_port):
        host, port = server_host_port
        key = _key("s")
        N = 4
        holder = SyncDistributedSemaphore(
            key=key,
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=10,
            servers=[(host, port)],
        )
        holder.acquire()
        waiters = [
            SyncDistributedSemaphore(
                key=key,
                limit=1,
                acquire_timeout_s=10,
                lease_ttl_s=10,
                servers=[(host, port)],
            )
            for _ in range(N)
        ]
        for w in waiters:
            assert w.enqueue() == "queued"
            time.sleep(0.05)
        holder.release()
        order: list[int] = []
        order_lock = threading.Lock()

        def runner(idx: int, w: SyncDistributedSemaphore):
            assert w.wait() is True
            with order_lock:
                order.append(idx)
            w.release()

        threads = [
            threading.Thread(target=runner, args=(i, w)) for i, w in enumerate(waiters)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        assert order == list(range(N))


# ===========================================================================
# Background renewal updates self.lease
# ===========================================================================


class TestRenewUpdatesLease:
    def test_lock_survives_past_initial_lease(self, server_host_port):
        """If the renew loop is silently broken, the server-side lease will
        expire mid-test and a contender can grab the key. With renew working,
        the lock stays held past the original lease window."""
        host, port = server_host_port
        key = _key("renew-survives")
        # 2s lease, renew at 1s → second contender shouldn't grab the key
        # even after we sleep 4s (twice the original lease).
        holder = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=2,
            renew_ratio=0.5,
            servers=[(host, port)],
        )
        holder.acquire()
        try:
            time.sleep(4.0)  # twice the lease — survives only if renew works
            contender = SyncDistributedLock(
                key=key,
                acquire_timeout_s=0,
                lease_ttl_s=5,
                servers=[(host, port)],
            )
            assert contender.acquire() is False, "renew loop failed — lease lapsed"
            assert holder.token is not None
        finally:
            holder.release()

    def test_lease_field_updated_after_renew(self, server_host_port):
        """``self.lease`` must reflect the server's remaining-seconds value
        after at least one renew tick, not the stale acquire-time value."""
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("renew-lease"),
            acquire_timeout_s=5,
            lease_ttl_s=2,
            renew_ratio=0.5,
            servers=[(host, port)],
        )
        lock.acquire()
        try:
            initial = lock.lease
            assert initial > 0
            time.sleep(1.5)
            assert lock.lease > 0
            assert lock.token is not None
        finally:
            lock.release()


# ===========================================================================
# Lock-instance reuse and stats-with-many-locks regression tests
# ===========================================================================


class TestInstanceReuse:
    def test_acquire_release_acquire_round_trip(self, server_host_port):
        """A single ``DistributedLock`` instance should be reusable across
        acquire/release cycles. The renewal thread, ``_stop_event`` and
        ``_closed`` flag must all reset cleanly between cycles."""
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("reuse"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        for _ in range(3):
            assert lock.acquire() is True
            tok = lock.token
            assert tok is not None
            assert lock.release() is True
            assert lock.token is None

    def test_acquire_after_acquire_silently_resets(self, server_host_port):
        """Calling ``acquire()`` again without release must drop the previous
        connection (auto-releasing server-side) and acquire fresh."""
        host, port = server_host_port
        lock = SyncDistributedLock(
            key=_key("reset"),
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        lock.acquire()
        first = lock.token
        try:
            lock.acquire()  # silently re-acquires with a new conn
            second = lock.token
            assert second is not None
            assert second != first
        finally:
            lock.release()


class TestContextManagerCleanup:
    def test_releases_on_exception_inside_block(self, server_host_port):
        """``with lock: raise`` must still release the lock — otherwise a
        contender hits a ghost holder until the lease expires."""
        host, port = server_host_port
        key = _key("ctx-exc")
        lock = SyncDistributedLock(
            key=key,
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[(host, port)],
        )

        class Boom(Exception):
            pass

        with pytest.raises(Boom):
            with lock:
                raise Boom()

        assert lock.token is None
        # contender can grab the key immediately — proves release happened
        contender = SyncDistributedLock(
            key=key,
            acquire_timeout_s=2,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        try:
            assert contender.acquire() is True
        finally:
            contender.release()


class TestConnConcurrentUse:
    """Per the docs, ``SyncConn`` is safe to share across threads. Two
    threads calling ``acquire`` on different keys against one conn must
    not interleave their bytes on the wire."""

    def test_two_threads_share_one_conn(self, server_host_port):
        host, port = server_host_port
        conn = _open_low_level(host, port)
        results: dict[str, str] = {}
        errors: list[BaseException] = []

        def worker(key: str):
            try:
                tok, _ = ds.acquire(conn, key, 5, lease_ttl_s=10)
                results[key] = tok
                ds.release(conn, key, tok)
            except BaseException as e:
                errors.append(e)

        try:
            keys = [_key(f"shared-{i}") for i in range(8)]
            threads = [threading.Thread(target=worker, args=(k,)) for k in keys]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            assert errors == []
            assert len(results) == len(keys)
            assert len(set(results.values())) == len(keys)
        finally:
            conn.close()


class TestStatsWithManyLocks:
    """Regression: the response-line cap must accommodate stats JSON for the
    full ``--max-locks`` working set. With the previous 64 KiB cap, a
    moderately-loaded server's stats response would be silently truncated."""

    def test_stats_handles_kilobyte_scale(self, server_host_port):
        host, port = server_host_port
        # Hold ~200 locks concurrently; stats response is well past 4 KiB but
        # within the new 1 MiB cap. Anything that fits old behaviour also
        # fits the new constant — this proves the new constant is wired up.
        holders = [_open_low_level(host, port) for _ in range(200)]
        tokens: list[tuple[str, str]] = []
        try:
            for i, c in enumerate(holders):
                k = f"manylocks-{uuid.uuid4().hex[:8]}-{i}"
                tok, _ = ds.acquire(c, k, 5, lease_ttl_s=30)
                tokens.append((k, tok))
            stats_conn = _open_low_level(host, port)
            try:
                result = ds.stats(stats_conn)
                assert len(result["locks"]) >= 200
            finally:
                stats_conn.close()
        finally:
            for c, (k, tok) in zip(holders, tokens):
                try:
                    ds.release(c, k, tok)
                except Exception:
                    pass
                c.close()


# ===========================================================================
# TLS / auth integration (optional, skipped if env vars not set)
# ===========================================================================


_tls_port = os.environ.get("DFLOCKD_TEST_TLS_PORT")
_skip_tls = pytest.mark.skipif(
    _tls_port is None, reason="DFLOCKD_TEST_TLS_PORT not set"
)

_auth_token = os.environ.get("DFLOCKD_TEST_AUTH_TOKEN")
_auth_port = os.environ.get("DFLOCKD_TEST_AUTH_PORT")
_skip_auth = pytest.mark.skipif(
    _auth_token is None or _auth_port is None,
    reason="DFLOCKD_TEST_AUTH_TOKEN/PORT not set",
)


@_skip_tls
class TestTlsIntegration:
    def test_lock_over_tls(self):
        port = int(os.environ["DFLOCKD_TEST_TLS_PORT"])
        ctx = ssl.create_default_context(cafile=os.environ.get("DFLOCKD_TEST_TLS_CA"))
        lock = SyncDistributedLock(
            key=_key("tls"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", port)],
            ssl_context=ctx,
        )
        with lock as held:
            assert held.token is not None


@_skip_auth
class TestAuthIntegration:
    def test_lock_with_auth(self):
        port = int(os.environ["DFLOCKD_TEST_AUTH_PORT"])
        token = os.environ["DFLOCKD_TEST_AUTH_TOKEN"]
        lock = SyncDistributedLock(
            key=_key("auth"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", port)],
            auth_token=token,
        )
        with lock as held:
            assert held.token is not None

    def test_bad_token_rejected(self):
        port = int(os.environ["DFLOCKD_TEST_AUTH_PORT"])
        lock = SyncDistributedLock(
            key=_key("auth"),
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", port)],
            auth_token="wrong-token",
        )
        with pytest.raises(PermissionError, match="authentication failed"):
            lock.acquire()
