"""Integration tests: async client and sync client against a running dflockd server."""

import asyncio
import io
import os
import socket
import ssl
import threading
import time

import pytest

import dflockd_client.client as aclient
import dflockd_client.sync_client as sclient

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _open(host: str, port: int):
    return await asyncio.open_connection(host, port)


def _sync_connect(host: str, port: int) -> tuple[socket.socket, io.TextIOWrapper]:
    sock = socket.create_connection((host, port))
    rfile = sock.makefile("r", encoding="utf-8")
    return sock, rfile


# ===========================================================================
# Validation: cmd_prefix and limit must agree on lock-vs-semaphore
# ===========================================================================


class TestCmdPrefixLimitInvariant:
    """The unified acquire/enqueue protocol functions must reject mismatched
    (cmd_prefix, limit) pairs. Without this check, a lock acquire with
    `limit=N` was silently sent as `l <key> <timeout> <N>` which the server
    parsed as `<lease_ttl>=N`, not as a limit — producing wrong protocol
    behavior with no visible error."""

    async def test_async_acquire_lock_with_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            await aclient.acquire(None, None, "k", 1, limit=5)  # type: ignore[arg-type]

    async def test_async_acquire_sem_without_limit_rejected(self):
        with pytest.raises(ValueError, match="limit is required"):
            await aclient.acquire(None, None, "k", 1, cmd_prefix="s")  # type: ignore[arg-type]

    async def test_async_enqueue_lock_with_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            await aclient.enqueue(None, None, "k", limit=5)  # type: ignore[arg-type]

    async def test_async_enqueue_sem_without_limit_rejected(self):
        with pytest.raises(ValueError, match="limit is required"):
            await aclient.enqueue(None, None, "k", cmd_prefix="s")  # type: ignore[arg-type]

    async def test_async_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            await aclient.acquire(None, None, "k", 1, cmd_prefix="x")  # type: ignore[arg-type]

    async def test_async_renew_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            await aclient.renew(None, None, "k", "tok", cmd_prefix="sem")  # type: ignore[arg-type]

    async def test_async_wait_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            await aclient.wait(None, None, "k", 1, cmd_prefix="x")  # type: ignore[arg-type]

    async def test_async_release_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            await aclient.release(None, None, "k", "tok", cmd_prefix="x")  # type: ignore[arg-type]

    def test_sync_acquire_lock_with_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            sclient.acquire(None, None, "k", 1, limit=5)  # type: ignore[arg-type]

    def test_sync_acquire_sem_without_limit_rejected(self):
        with pytest.raises(ValueError, match="limit is required"):
            sclient.acquire(None, None, "k", 1, cmd_prefix="s")  # type: ignore[arg-type]

    def test_sync_enqueue_lock_with_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            sclient.enqueue(None, None, "k", limit=5)  # type: ignore[arg-type]

    def test_sync_enqueue_sem_without_limit_rejected(self):
        with pytest.raises(ValueError, match="limit is required"):
            sclient.enqueue(None, None, "k", cmd_prefix="s")  # type: ignore[arg-type]

    def test_sync_renew_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            sclient.renew(None, None, "k", "tok", cmd_prefix="sem")  # type: ignore[arg-type]

    def test_sync_wait_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            sclient.wait(None, None, "k", 1, cmd_prefix="x")  # type: ignore[arg-type]

    def test_sync_release_invalid_cmd_prefix_rejected(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            sclient.release(None, None, "k", "tok", cmd_prefix="x")  # type: ignore[arg-type]


# ===========================================================================
# Bug fix: self.lease must reflect the post-renew remaining seconds
# ===========================================================================


class TestRenewUpdatesLease:
    async def test_async_renew_loop_updates_self_lease(self):
        """After a successful renew, self.lease must be updated to the
        server's reported remaining seconds, not left stale at the
        initial-acquire value."""
        lock = aclient.DistributedLock("k", acquire_timeout_s=1, renew_ratio=0.5)
        lock.token = "fake-token"
        lock.lease = 60  # initial lease at acquire

        # Stub the protocol renew to return a different remaining value.
        async def fake_renew(reader, writer, token):
            return 42

        lock._proto_renew = fake_renew  # type: ignore[assignment]

        # Stub the writer/reader so the token-equality check inside the
        # loop's lease update can run.
        class _W:
            def __init__(self): self._closed = False
            def is_closing(self): return False
        lock._writer = _W()  # type: ignore[assignment]
        lock._reader = object()  # type: ignore[assignment]

        # Run one iteration of the renew loop body manually using a
        # short sleep, then cancel.
        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        # Patch sleep so we don't have to wait for the real interval.
        import dflockd_client.client as client_mod
        client_mod_asyncio_sleep = client_mod.asyncio.sleep
        client_mod.asyncio.sleep = fast_sleep  # type: ignore[assignment]
        try:
            task = asyncio.create_task(lock._renew_loop())
            # Yield enough times for one renew tick to land.
            for _ in range(20):
                await original_sleep(0)
                if lock.lease == 42:
                    break
            task.cancel()
            # _renew_loop catches CancelledError and returns cleanly,
            # so the await doesn't raise — just wait for it to finish.
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            client_mod.asyncio.sleep = client_mod_asyncio_sleep

        assert lock.lease == 42, (
            f"lease should reflect post-renew remaining (got {lock.lease})"
        )

    def test_sync_renew_loop_updates_self_lease(self):
        lock = sclient.DistributedLock("k", acquire_timeout_s=1, renew_ratio=0.5)
        lock.token = "fake-token"
        lock.lease = 60

        # Stubs so the loop can step through one iteration.
        renew_called = threading.Event()

        def fake_renew(sock, rfile, token):
            renew_called.set()
            return 42

        lock._proto_renew = fake_renew  # type: ignore[assignment]

        # Make the loop think we have an active connection.
        class _S:
            def gettimeout(self): return None
            def settimeout(self, _): pass
        lock._sock = _S()  # type: ignore[assignment]
        lock._rfile = io.StringIO("")  # type: ignore[assignment]

        # Stop event with very short interval so we get an immediate tick.
        # renew_ratio=0.5 * lease=60 = 30s; clamp to 0.001 by stubbing the
        # _stop_event.wait method.
        original_wait = lock._stop_event.wait

        def fast_wait(timeout):
            return original_wait(0.01)

        lock._stop_event.wait = fast_wait  # type: ignore[assignment]

        t = threading.Thread(target=lock._renew_loop, daemon=True)
        t.start()
        renew_called.wait(timeout=2)
        # Allow the post-renew lease update to land.
        time.sleep(0.05)
        lock._stop_event.set()
        t.join(timeout=2)

        assert lock.lease == 42, (
            f"lease should reflect post-renew remaining (got {lock.lease})"
        )


# ===========================================================================
# Bug fix: async release() must time out instead of hanging on a stuck server
# ===========================================================================


class TestAsyncReleaseTimeout:
    async def test_release_times_out_when_server_hangs(self):
        """If _proto_release blocks forever (server unresponsive but TCP
        still open), release() must surface that as a logged failure and
        return False rather than wedging the caller."""
        lock = aclient.DistributedLock("k", acquire_timeout_s=1)
        lock.token = "fake-token"
        lock.lease = 60

        class _W:
            def is_closing(self): return False
            def close(self): pass
            async def wait_closed(self): return
        lock._writer = _W()  # type: ignore[assignment]
        lock._reader = object()  # type: ignore[assignment]

        # Stub _proto_release so it never returns.
        proto_started = asyncio.Event()

        async def hung_release(reader, writer, token):
            proto_started.set()
            await asyncio.sleep(3600)

        lock._proto_release = hung_release  # type: ignore[assignment]

        # Patch the I/O slack constant to something tiny so the test runs
        # quickly. The release path uses _IO_TIMEOUT_SLACK_S directly.
        import dflockd_client.client as client_mod
        original_slack = client_mod._IO_TIMEOUT_SLACK_S
        client_mod._IO_TIMEOUT_SLACK_S = 0.1  # type: ignore[assignment]
        try:
            start = asyncio.get_running_loop().time()
            result = await lock.release()
            elapsed = asyncio.get_running_loop().time() - start
        finally:
            client_mod._IO_TIMEOUT_SLACK_S = original_slack  # type: ignore[assignment]

        assert proto_started.is_set(), "release should have invoked _proto_release"
        # The release attempt timed out so it returns False (not raise).
        assert result is False
        # And the caller wasn't wedged for long.
        assert elapsed < 2, f"release blocked for {elapsed}s; should have timed out fast"


# ===========================================================================
# Async client — low-level functions
# ===========================================================================


class TestAsyncAcquireRelease:
    async def test_acquire_and_release(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            token, lease = await aclient.acquire(reader, writer, "k1", 5)
            assert isinstance(token, str) and len(token) > 0
            assert lease > 0
            await aclient.release(reader, writer, "k1", token)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_acquire_timeout(self, server_host_port):
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        r2, w2 = await _open(host, port)
        try:
            await aclient.acquire(r1, w1, "k1", 5)
            with pytest.raises(TimeoutError):
                await aclient.acquire(r2, w2, "k1", 0)
        finally:
            w1.close()
            w2.close()

    async def test_release_bad_token(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            await aclient.acquire(reader, writer, "k1", 5)
            with pytest.raises(RuntimeError, match="release failed"):
                await aclient.release(reader, writer, "k1", "badtoken")
        finally:
            writer.close()


class TestAsyncRenew:
    async def test_renew(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            token, _lease = await aclient.acquire(
                reader, writer, "k1", 5, lease_ttl_s=10
            )
            remaining = await aclient.renew(reader, writer, "k1", token, lease_ttl_s=20)
            assert remaining >= 0
        finally:
            writer.close()

    async def test_renew_bad_token(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            await aclient.acquire(reader, writer, "k1", 5)
            with pytest.raises(RuntimeError, match="renew failed"):
                await aclient.renew(reader, writer, "k1", "badtoken")
        finally:
            writer.close()


# ===========================================================================
# Async DistributedLock context manager
# ===========================================================================


class TestAsyncDistributedLock:
    async def test_context_manager(self, server_host_port):
        host, port = server_host_port
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
            renew_ratio=0.3,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None

    async def test_acquire_release_methods(self, server_host_port):
        host, port = server_host_port
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        ok = await lock.acquire()
        assert ok is True
        assert lock.token is not None
        await lock.release()
        assert lock.token is None

    async def test_acquire_timeout_returns_false(self, server_host_port):
        host, port = server_host_port
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=0,
            lease_ttl_s=30,
            servers=[(host, port)],
        )
        await lock1.acquire()
        try:
            ok = await lock2.acquire()
            assert ok is False
        finally:
            await lock1.release()

    async def test_mutual_exclusion(self, server_host_port):
        """Two async locks on the same key; second must wait."""
        host, port = server_host_port
        results: list[int] = []

        async def worker(n: int, timeout: int):
            lock = aclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[(host, port)],
            )
            async with lock:
                results.append(n)
                await asyncio.sleep(0.1)

        t1 = asyncio.create_task(worker(1, 5))
        await asyncio.sleep(0.02)
        t2 = asyncio.create_task(worker(2, 5))
        await asyncio.gather(t1, t2)
        assert results == [1, 2]


# ===========================================================================
# Sync client — low-level functions
# ===========================================================================


class TestSyncAcquireRelease:
    async def test_acquire_and_release(self, server_host_port):
        """Run sync client in a thread against the server."""
        host, port = server_host_port

        def _work():
            sock, rfile = _sync_connect(host, port)
            try:
                token, lease = sclient.acquire(sock, rfile, "k1", 5)
                assert isinstance(token, str) and len(token) > 0
                assert lease > 0
                sclient.release(sock, rfile, "k1", token)
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)

    async def test_renew(self, server_host_port):
        host, port = server_host_port

        def _work():
            sock, rfile = _sync_connect(host, port)
            try:
                token, _ = sclient.acquire(sock, rfile, "k1", 5, lease_ttl_s=10)
                remaining = sclient.renew(sock, rfile, "k1", token, lease_ttl_s=20)
                assert remaining >= 0
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)


# ===========================================================================
# Sync DistributedLock context manager
# ===========================================================================


class TestSyncDistributedLock:
    async def test_context_manager(self, server_host_port):
        host, port = server_host_port

        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[(host, port)],
                renew_ratio=0.3,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_acquire_release_methods(self, server_host_port):
        host, port = server_host_port

        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[(host, port)],
            )
            ok = lock.acquire()
            assert ok is True
            assert lock.token is not None
            lock.release()
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_mutual_exclusion_threads(self, server_host_port):
        """Two sync locks from different threads; verify serial execution."""
        host, port = server_host_port
        results: list[int] = []
        lock_obj = threading.Lock()

        def _worker(n, timeout):
            lock = sclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[(host, port)],
            )
            with lock:
                with lock_obj:
                    results.append(n)
                time.sleep(0.1)

        def _run():
            t1 = threading.Thread(target=_worker, args=(1, 5))
            t2 = threading.Thread(target=_worker, args=(2, 5))
            t1.start()
            time.sleep(0.02)
            t2.start()
            t1.join(timeout=10)
            t2.join(timeout=10)

        await asyncio.to_thread(_run)
        assert results == [1, 2]


# ===========================================================================
# Disconnect behavior
# ===========================================================================


class TestDisconnectBehavior:
    async def test_disconnect_releases_lock(self, server_host_port):
        """When a client disconnects, the server releases its lock."""
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        await aclient.acquire(r1, w1, "k1", 5, lease_ttl_s=30)

        # Disconnect abruptly
        w1.close()
        await asyncio.sleep(0.2)

        # Now a second client should be able to acquire immediately
        r2, w2 = await _open(host, port)
        try:
            token2, _ = await aclient.acquire(r2, w2, "k1", 1, lease_ttl_s=30)
            assert token2 is not None
        finally:
            w2.close()


# ===========================================================================
# Async two-phase: enqueue + wait
# ===========================================================================


class TestAsyncTwoPhase:
    async def test_low_level_enqueue_wait_release(self, server_host_port):
        """Low-level: enqueue + wait + release on a free lock."""
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            status, token, lease = await aclient.enqueue(reader, writer, "k1")
            assert status == "acquired"
            assert token is not None
            assert lease > 0

            # wait should return immediately (already acquired)
            tok, ttl = await aclient.wait(reader, writer, "k1", 5)
            assert tok == token

            await aclient.release(reader, writer, "k1", tok)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_low_level_queued_then_wait(self, server_host_port):
        """Low-level: conn1 holds, conn2 enqueues (queued), conn1 releases, conn2 waits."""
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        r2, w2 = await _open(host, port)
        try:
            # conn1 acquires
            tok1, _ = await aclient.acquire(r1, w1, "k1", 5)

            # conn2 enqueues — should be queued
            status, _, _ = await aclient.enqueue(r2, w2, "k1")
            assert status == "queued"

            # Release conn1 in background, then conn2 waits
            async def _release_soon():
                await asyncio.sleep(0.1)
                await aclient.release(r1, w1, "k1", tok1)

            release_task = asyncio.create_task(_release_soon())
            tok2, lease2 = await aclient.wait(r2, w2, "k1", 5)
            await release_task

            assert tok2 is not None
            assert lease2 > 0
            await aclient.release(r2, w2, "k1", tok2)
        finally:
            w1.close()
            w2.close()

    async def test_distributed_lock_two_phase(self, server_host_port):
        """DistributedLock.enqueue() + wait() + release() flow."""
        host, port = server_host_port
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        status = await lock.enqueue()
        assert status == "acquired"
        assert lock.token is not None

        ok = await lock.wait()
        assert ok is True

        await lock.release()
        assert lock.token is None

    async def test_fifo_ordering_among_tasks(self, server_host_port):
        """5 waiters enqueued sequentially must be granted in FIFO order."""
        host, port = server_host_port
        N = 5
        grant_order: list[int] = []

        # Holder keeps the lock while all waiters enqueue
        holder = aclient.DistributedLock(
            key="fifo_lock",
            acquire_timeout_s=5,
            lease_ttl_s=10,
            servers=[(host, port)],
        )
        await holder.acquire()

        # Create N locks and enqueue them sequentially
        waiters: list[aclient.DistributedLock] = []
        for i in range(N):
            lk = aclient.DistributedLock(
                key="fifo_lock",
                acquire_timeout_s=10,
                lease_ttl_s=10,
                servers=[(host, port)],
            )
            status = await lk.enqueue()
            assert status == "queued"
            waiters.append(lk)
            await asyncio.sleep(0.05)

        # Release the holder so waiters can be granted
        await holder.release()

        # All waiters call wait() concurrently; record grant order
        async def _wait_and_record(idx: int, lk: aclient.DistributedLock):
            ok = await lk.wait()
            assert ok is True
            grant_order.append(idx)
            await lk.release()

        await asyncio.gather(*[_wait_and_record(i, w) for i, w in enumerate(waiters)])
        assert grant_order == list(range(N))

    async def test_distributed_lock_two_phase_contention(self, server_host_port):
        """Two DistributedLock instances: lock1 holds, lock2 does enqueue+wait."""
        host, port = server_host_port
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )

        await lock1.acquire()

        status = await lock2.enqueue()
        assert status == "queued"

        async def _release_soon():
            await asyncio.sleep(0.1)
            await lock1.release()

        release_task = asyncio.create_task(_release_soon())
        ok = await lock2.wait()
        await release_task

        assert ok is True
        assert lock2.token is not None
        await lock2.release()


# ===========================================================================
# Async semaphore — low-level functions
# ===========================================================================


class TestAsyncSemAcquireRelease:
    async def test_acquire_and_release(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            token, lease = await aclient.sem_acquire(reader, writer, "s1", 5, 2)
            assert isinstance(token, str) and len(token) > 0
            assert lease > 0
            await aclient.sem_release(reader, writer, "s1", token)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_acquire_timeout(self, server_host_port):
        """Semaphore with limit=1 should timeout on the second acquire."""
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        r2, w2 = await _open(host, port)
        try:
            await aclient.sem_acquire(r1, w1, "s1_lim1", 5, 1)
            with pytest.raises(TimeoutError):
                await aclient.sem_acquire(r2, w2, "s1_lim1", 0, 1)
        finally:
            w1.close()
            w2.close()

    async def test_release_bad_token(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            await aclient.sem_acquire(reader, writer, "s1", 5, 2)
            with pytest.raises(RuntimeError, match="sem_release failed"):
                await aclient.sem_release(reader, writer, "s1", "badtoken")
        finally:
            writer.close()


class TestAsyncSemRenew:
    async def test_renew(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            token, _lease = await aclient.sem_acquire(
                reader, writer, "s1", 5, 2, lease_ttl_s=10
            )
            remaining = await aclient.sem_renew(
                reader, writer, "s1", token, lease_ttl_s=20
            )
            assert remaining >= 0
        finally:
            writer.close()

    async def test_renew_bad_token(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            await aclient.sem_acquire(reader, writer, "s1", 5, 2)
            with pytest.raises(RuntimeError, match="sem_renew failed"):
                await aclient.sem_renew(reader, writer, "s1", "badtoken")
        finally:
            writer.close()


# ===========================================================================
# Async DistributedSemaphore
# ===========================================================================


class TestAsyncDistributedSemaphore:
    async def test_context_manager(self, server_host_port):
        host, port = server_host_port
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
            renew_ratio=0.3,
        )
        async with sem as s:
            assert s.token is not None
        assert sem.token is None

    async def test_acquire_release_methods(self, server_host_port):
        host, port = server_host_port
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        ok = await sem.acquire()
        assert ok is True
        assert sem.token is not None
        await sem.release()
        assert sem.token is None

    async def test_concurrency_within_limit(self, server_host_port):
        """Semaphore with limit=2 allows 2 concurrent holders but blocks a 3rd."""
        host, port = server_host_port
        sem1 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        sem2 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        sem3 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=0,
            lease_ttl_s=5,
            servers=[(host, port)],
        )

        ok1 = await sem1.acquire()
        ok2 = await sem2.acquire()
        assert ok1 is True
        assert ok2 is True

        # Third should fail (timeout=0)
        ok3 = await sem3.acquire()
        assert ok3 is False

        await sem1.release()
        await sem2.release()


# ===========================================================================
# Async semaphore two-phase: enqueue + wait
# ===========================================================================


class TestAsyncSemTwoPhase:
    async def test_low_level_enqueue_wait_release(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            status, token, lease = await aclient.sem_enqueue(reader, writer, "s1", 2)
            assert status == "acquired"
            assert token is not None
            assert lease > 0

            tok, ttl = await aclient.sem_wait(reader, writer, "s1", 5)
            assert tok == token

            await aclient.sem_release(reader, writer, "s1", tok)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_low_level_queued_then_wait(self, server_host_port):
        """conn1 holds (limit=1), conn2 enqueues (queued), conn1 releases, conn2 waits."""
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        r2, w2 = await _open(host, port)
        try:
            tok1, _ = await aclient.sem_acquire(r1, w1, "s1_queue", 5, 1)

            status, _, _ = await aclient.sem_enqueue(r2, w2, "s1_queue", 1)
            assert status == "queued"

            async def _release_soon():
                await asyncio.sleep(0.1)
                await aclient.sem_release(r1, w1, "s1_queue", tok1)

            release_task = asyncio.create_task(_release_soon())
            tok2, lease2 = await aclient.sem_wait(r2, w2, "s1_queue", 5)
            await release_task

            assert tok2 is not None
            assert lease2 > 0
            await aclient.sem_release(r2, w2, "s1_queue", tok2)
        finally:
            w1.close()
            w2.close()

    async def test_distributed_semaphore_two_phase(self, server_host_port):
        host, port = server_host_port
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        status = await sem.enqueue()
        assert status == "acquired"
        assert sem.token is not None

        ok = await sem.wait()
        assert ok is True

        await sem.release()
        assert sem.token is None

    async def test_fifo_ordering_among_tasks(self, server_host_port):
        """5 waiters enqueued sequentially must be granted in FIFO order (semaphore, limit=1)."""
        host, port = server_host_port
        N = 5
        grant_order: list[int] = []

        holder = aclient.DistributedSemaphore(
            key="fifo_sem",
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=10,
            servers=[(host, port)],
        )
        await holder.acquire()

        waiters: list[aclient.DistributedSemaphore] = []
        for i in range(N):
            sem = aclient.DistributedSemaphore(
                key="fifo_sem",
                limit=1,
                acquire_timeout_s=10,
                lease_ttl_s=10,
                servers=[(host, port)],
            )
            status = await sem.enqueue()
            assert status == "queued"
            waiters.append(sem)
            await asyncio.sleep(0.05)

        await holder.release()

        async def _wait_and_record(idx: int, sem: aclient.DistributedSemaphore):
            ok = await sem.wait()
            assert ok is True
            grant_order.append(idx)
            await sem.release()

        await asyncio.gather(*[_wait_and_record(i, w) for i, w in enumerate(waiters)])
        assert grant_order == list(range(N))

    async def test_distributed_semaphore_two_phase_contention(self, server_host_port):
        """sem1 holds (limit=1), sem2 enqueues+waits."""
        host, port = server_host_port
        sem1 = aclient.DistributedSemaphore(
            key="s1_2phase",
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )
        sem2 = aclient.DistributedSemaphore(
            key="s1_2phase",
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[(host, port)],
        )

        await sem1.acquire()

        status = await sem2.enqueue()
        assert status == "queued"

        async def _release_soon():
            await asyncio.sleep(0.1)
            await sem1.release()

        release_task = asyncio.create_task(_release_soon())
        ok = await sem2.wait()
        await release_task

        assert ok is True
        assert sem2.token is not None
        await sem2.release()


# ===========================================================================
# Async stats
# ===========================================================================


class TestAsyncStats:
    async def test_stats_empty(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            result = await aclient.stats(reader, writer)
            assert isinstance(result, dict)
            assert "connections" in result
            assert "locks" in result
            assert "semaphores" in result
            assert "idle_locks" in result
            assert "idle_semaphores" in result
            assert isinstance(result["connections"], int)
            assert isinstance(result["locks"], list)
            assert isinstance(result["semaphores"], list)
        finally:
            writer.close()
            await writer.wait_closed()

    async def test_stats_with_held_lock(self, server_host_port):
        host, port = server_host_port
        r1, w1 = await _open(host, port)
        r2, w2 = await _open(host, port)
        try:
            token, _ = await aclient.acquire(r1, w1, "stats_lock", 5, lease_ttl_s=30)
            result = await aclient.stats(r2, w2)
            lock_keys = [lk["key"] for lk in result["locks"]]
            assert "stats_lock" in lock_keys
            await aclient.release(r1, w1, "stats_lock", token)
        finally:
            w1.close()
            w2.close()

    async def test_stats_includes_signal_channels(self, server_host_port):
        """The server returns `signal_channels` alongside the lock/sem
        stats. The TypedDict must include this key and `idle_locks`/
        `idle_semaphores` must be lists of dicts (with a `key` field),
        not bare strings — which is what the prior type annotation
        claimed."""
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            result = await aclient.stats(reader, writer)
            # The new field exists and is a list (possibly empty).
            assert "signal_channels" in result
            assert isinstance(result["signal_channels"], list)
            # Each idle entry, when present, is a dict — not a bare key string.
            for entry in result["idle_locks"]:
                assert isinstance(entry, dict)
                assert "key" in entry
            for entry in result["idle_semaphores"]:
                assert isinstance(entry, dict)
                assert "key" in entry
        finally:
            writer.close()
            await writer.wait_closed()


# ===========================================================================
# Sync stats (low-level)
# ===========================================================================


class TestSyncStats:
    async def test_stats_empty(self, server_host_port):
        host, port = server_host_port

        def _work():
            sock, rfile = _sync_connect(host, port)
            try:
                result = sclient.stats(sock, rfile)
                assert isinstance(result, dict)
                assert "connections" in result
                assert "locks" in result
                assert "semaphores" in result
                assert "idle_locks" in result
                assert "idle_semaphores" in result
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)

    async def test_stats_with_held_lock(self, server_host_port):
        host, port = server_host_port

        def _work():
            s1, r1 = _sync_connect(host, port)
            s2, r2 = _sync_connect(host, port)
            try:
                token, _ = sclient.acquire(s1, r1, "stats_lock", 5, lease_ttl_s=30)
                result = sclient.stats(s2, r2)
                lock_keys = [lk["key"] for lk in result["locks"]]
                assert "stats_lock" in lock_keys
                sclient.release(s1, r1, "stats_lock", token)
            finally:
                r1.close()
                s1.close()
                r2.close()
                s2.close()

        await asyncio.to_thread(_work)


class TestAsyncSharding:
    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            aclient.DistributedLock(key="k", servers=[])

    def test_default_servers(self):
        lock = aclient.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    async def test_custom_strategy(self, server_host_port):
        host, port = server_host_port
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            servers=[(host, port)],
            sharding_strategy=lambda key, n: 0,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None


# ===========================================================================
# TLS / ssl_context
# ===========================================================================


class TestAsyncAuthTokenDefaults:
    def test_lock_auth_token_default_none(self):
        lock = aclient.DistributedLock(key="k")
        assert lock.auth_token is None

    def test_semaphore_auth_token_default_none(self):
        sem = aclient.DistributedSemaphore(key="k", limit=2)
        assert sem.auth_token is None

    def test_lock_auth_token_set(self):
        lock = aclient.DistributedLock(key="k", auth_token="secret")
        assert lock.auth_token == "secret"

    def test_semaphore_auth_token_set(self):
        sem = aclient.DistributedSemaphore(key="k", limit=2, auth_token="secret")
        assert sem.auth_token == "secret"


class TestSyncAuthTokenDefaults:
    def test_lock_auth_token_default_none(self):
        lock = sclient.DistributedLock(key="k")
        assert lock.auth_token is None

    def test_semaphore_auth_token_default_none(self):
        sem = sclient.DistributedSemaphore(key="k", limit=2)
        assert sem.auth_token is None

    def test_lock_auth_token_set(self):
        lock = sclient.DistributedLock(key="k", auth_token="secret")
        assert lock.auth_token == "secret"

    def test_semaphore_auth_token_set(self):
        sem = sclient.DistributedSemaphore(key="k", limit=2, auth_token="secret")
        assert sem.auth_token == "secret"


class TestAsyncSslContextDefaults:
    def test_lock_ssl_context_default_none(self):
        lock = aclient.DistributedLock(key="k")
        assert lock.ssl_context is None

    def test_semaphore_ssl_context_default_none(self):
        sem = aclient.DistributedSemaphore(key="k", limit=2)
        assert sem.ssl_context is None

    def test_lock_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        lock = aclient.DistributedLock(key="k", ssl_context=ctx)
        assert lock.ssl_context is ctx

    def test_semaphore_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sem = aclient.DistributedSemaphore(key="k", limit=2, ssl_context=ctx)
        assert sem.ssl_context is ctx


class TestSyncSslContextDefaults:
    def test_lock_ssl_context_default_none(self):
        lock = sclient.DistributedLock(key="k")
        assert lock.ssl_context is None

    def test_semaphore_ssl_context_default_none(self):
        sem = sclient.DistributedSemaphore(key="k", limit=2)
        assert sem.ssl_context is None

    def test_lock_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        lock = sclient.DistributedLock(key="k", ssl_context=ctx)
        assert lock.ssl_context is ctx

    def test_semaphore_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sem = sclient.DistributedSemaphore(key="k", limit=2, ssl_context=ctx)
        assert sem.ssl_context is ctx


# ===========================================================================
# TLS integration tests (requires DFLOCKD_TEST_TLS_PORT)
# ===========================================================================

_tls_port = os.environ.get("DFLOCKD_TEST_TLS_PORT")
_skip_tls = pytest.mark.skipif(
    _tls_port is None, reason="DFLOCKD_TEST_TLS_PORT not set"
)


@_skip_tls
class TestAsyncTlsIntegration:
    @pytest.fixture()
    def tls_port(self):
        return int(os.environ["DFLOCKD_TEST_TLS_PORT"])

    @pytest.fixture()
    def tls_context(self):
        cafile = os.environ.get("DFLOCKD_TEST_TLS_CA")
        return ssl.create_default_context(cafile=cafile)

    async def test_lock_context_manager(self, tls_port, tls_context):
        lock = aclient.DistributedLock(
            key="tls_k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", tls_port)],
            ssl_context=tls_context,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None

    async def test_semaphore_context_manager(self, tls_port, tls_context):
        sem = aclient.DistributedSemaphore(
            key="tls_s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", tls_port)],
            ssl_context=tls_context,
        )
        async with sem as s:
            assert s.token is not None
        assert sem.token is None


@_skip_tls
class TestSyncTlsIntegration:
    @pytest.fixture()
    def tls_port(self):
        return int(os.environ["DFLOCKD_TEST_TLS_PORT"])

    @pytest.fixture()
    def tls_context(self):
        cafile = os.environ.get("DFLOCKD_TEST_TLS_CA")
        return ssl.create_default_context(cafile=cafile)

    async def test_lock_context_manager(self, tls_port, tls_context):
        def _work():
            lock = sclient.DistributedLock(
                key="tls_k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", tls_port)],
                ssl_context=tls_context,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_semaphore_context_manager(self, tls_port, tls_context):
        def _work():
            sem = sclient.DistributedSemaphore(
                key="tls_s1",
                limit=2,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", tls_port)],
                ssl_context=tls_context,
            )
            with sem as s:
                assert s.token is not None
            assert sem.token is None

        await asyncio.to_thread(_work)


# ===========================================================================
# Auth token integration tests (requires DFLOCKD_TEST_AUTH_TOKEN + DFLOCKD_TEST_AUTH_PORT)
# ===========================================================================

_auth_token = os.environ.get("DFLOCKD_TEST_AUTH_TOKEN")
_auth_port = os.environ.get("DFLOCKD_TEST_AUTH_PORT")
_skip_auth = pytest.mark.skipif(
    _auth_token is None or _auth_port is None,
    reason="DFLOCKD_TEST_AUTH_TOKEN and DFLOCKD_TEST_AUTH_PORT not set",
)


@_skip_auth
class TestAsyncAuthIntegration:
    @pytest.fixture()
    def auth_port(self):
        return int(os.environ["DFLOCKD_TEST_AUTH_PORT"])

    @pytest.fixture()
    def auth_token(self):
        return os.environ["DFLOCKD_TEST_AUTH_TOKEN"]

    async def test_lock_with_auth(self, auth_port, auth_token):
        lock = aclient.DistributedLock(
            key="auth_k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", auth_port)],
            auth_token=auth_token,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None

    async def test_semaphore_with_auth(self, auth_port, auth_token):
        sem = aclient.DistributedSemaphore(
            key="auth_s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", auth_port)],
            auth_token=auth_token,
        )
        async with sem as s:
            assert s.token is not None
        assert sem.token is None

    async def test_lock_bad_token_raises(self, auth_port):
        lock = aclient.DistributedLock(
            key="auth_k1",
            acquire_timeout_s=5,
            servers=[("127.0.0.1", auth_port)],
            auth_token="wrong-token",
        )
        with pytest.raises(PermissionError, match="authentication failed"):
            await lock.acquire()

    async def test_semaphore_bad_token_raises(self, auth_port):
        sem = aclient.DistributedSemaphore(
            key="auth_s1",
            limit=2,
            acquire_timeout_s=5,
            servers=[("127.0.0.1", auth_port)],
            auth_token="wrong-token",
        )
        with pytest.raises(PermissionError, match="authentication failed"):
            await sem.acquire()


@_skip_auth
class TestSyncAuthIntegration:
    @pytest.fixture()
    def auth_port(self):
        return int(os.environ["DFLOCKD_TEST_AUTH_PORT"])

    @pytest.fixture()
    def auth_token(self):
        return os.environ["DFLOCKD_TEST_AUTH_TOKEN"]

    async def test_lock_with_auth(self, auth_port, auth_token):
        def _work():
            lock = sclient.DistributedLock(
                key="auth_k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", auth_port)],
                auth_token=auth_token,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_semaphore_with_auth(self, auth_port, auth_token):
        def _work():
            sem = sclient.DistributedSemaphore(
                key="auth_s1",
                limit=2,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", auth_port)],
                auth_token=auth_token,
            )
            with sem as s:
                assert s.token is not None
            assert sem.token is None

        await asyncio.to_thread(_work)

    async def test_lock_bad_token_raises(self, auth_port):
        def _work():
            lock = sclient.DistributedLock(
                key="auth_k1",
                acquire_timeout_s=5,
                servers=[("127.0.0.1", auth_port)],
                auth_token="wrong-token",
            )
            with pytest.raises(PermissionError, match="authentication failed"):
                lock.acquire()

        await asyncio.to_thread(_work)

    async def test_semaphore_bad_token_raises(self, auth_port):
        def _work():
            sem = sclient.DistributedSemaphore(
                key="auth_s1",
                limit=2,
                acquire_timeout_s=5,
                servers=[("127.0.0.1", auth_port)],
                auth_token="wrong-token",
            )
            with pytest.raises(PermissionError, match="authentication failed"):
                sem.acquire()

        await asyncio.to_thread(_work)
