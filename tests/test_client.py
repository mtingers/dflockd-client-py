"""Integration tests: async client and sync client against a running dflockd server."""

import asyncio
import socket
import threading
import time

import pytest

import dflockd_client.client as aclient
import dflockd_client.sync_client as sclient

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


async def _open(port: int):
    return await asyncio.open_connection("127.0.0.1", port)


def _sync_connect(port: int) -> tuple[socket.socket, ...]:
    sock = socket.create_connection(("127.0.0.1", port))
    rfile = sock.makefile("r", encoding="utf-8")
    return sock, rfile


# ===========================================================================
# Async client — low-level functions
# ===========================================================================


class TestAsyncAcquireRelease:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, server_port):
        reader, writer = await _open(server_port)
        try:
            token, lease = await aclient.acquire(reader, writer, "k1", 5)
            assert isinstance(token, str) and len(token) > 0
            assert lease > 0
            await aclient.release(reader, writer, "k1", token)
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_acquire_timeout(self, server_port):
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
        try:
            await aclient.acquire(r1, w1, "k1", 5)
            with pytest.raises(TimeoutError):
                await aclient.acquire(r2, w2, "k1", 0)
        finally:
            w1.close()
            w2.close()

    @pytest.mark.asyncio
    async def test_release_bad_token(self, server_port):
        reader, writer = await _open(server_port)
        try:
            await aclient.acquire(reader, writer, "k1", 5)
            with pytest.raises(RuntimeError, match="release failed"):
                await aclient.release(reader, writer, "k1", "badtoken")
        finally:
            writer.close()


class TestAsyncRenew:
    @pytest.mark.asyncio
    async def test_renew(self, server_port):
        reader, writer = await _open(server_port)
        try:
            token, _lease = await aclient.acquire(
                reader, writer, "k1", 5, lease_ttl_s=10
            )
            remaining = await aclient.renew(reader, writer, "k1", token, lease_ttl_s=20)
            assert remaining >= 0
        finally:
            writer.close()

    @pytest.mark.asyncio
    async def test_renew_bad_token(self, server_port):
        reader, writer = await _open(server_port)
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
    @pytest.mark.asyncio
    async def test_context_manager(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
            renew_ratio=0.3,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_acquire_release_methods(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        ok = await lock.acquire()
        assert ok is True
        assert lock.token is not None
        await lock.release()
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_acquire_timeout_returns_false(self, server_port):
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=30,
            servers=[("127.0.0.1", server_port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=0,
            lease_ttl_s=30,
            servers=[("127.0.0.1", server_port)],
        )
        await lock1.acquire()
        try:
            ok = await lock2.acquire()
            assert ok is False
        finally:
            await lock1.release()

    @pytest.mark.asyncio
    async def test_mutual_exclusion(self, server_port):
        """Two async locks on the same key; second must wait."""
        results: list[int] = []

        async def worker(n: int, timeout: int):
            lock = aclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
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
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, server_port):
        """Run sync client in a thread against the server."""

        def _work():
            sock, rfile = _sync_connect(server_port)
            try:
                token, lease = sclient.acquire(sock, rfile, "k1", 5)
                assert isinstance(token, str) and len(token) > 0
                assert lease > 0
                sclient.release(sock, rfile, "k1", token)
            finally:
                rfile.close()
                sock.close()

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_renew(self, server_port):
        def _work():
            sock, rfile = _sync_connect(server_port)
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
    @pytest.mark.asyncio
    async def test_context_manager(self, server_port):
        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
                renew_ratio=0.3,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_acquire_release_methods(self, server_port):
        def _work():
            lock = sclient.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            ok = lock.acquire()
            assert ok is True
            assert lock.token is not None
            lock.release()
            assert lock.token is None

        await asyncio.to_thread(_work)

    @pytest.mark.asyncio
    async def test_mutual_exclusion_threads(self, server_port):
        """Two sync locks from different threads; verify serial execution."""
        results: list[int] = []
        lock_obj = threading.Lock()

        def _worker(n, timeout):
            lock = sclient.DistributedLock(
                key="mutex",
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
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
    @pytest.mark.asyncio
    async def test_disconnect_releases_lock(self, server_port):
        """When a client disconnects, the server releases its lock."""
        r1, w1 = await _open(server_port)
        token1, _ = await aclient.acquire(r1, w1, "k1", 5, lease_ttl_s=30)

        # Disconnect abruptly
        w1.close()
        await asyncio.sleep(0.2)

        # Now a second client should be able to acquire immediately
        r2, w2 = await _open(server_port)
        try:
            token2, _ = await aclient.acquire(r2, w2, "k1", 1, lease_ttl_s=30)
            assert token2 is not None
        finally:
            w2.close()


# ===========================================================================
# Async two-phase: enqueue + wait
# ===========================================================================


class TestAsyncTwoPhase:
    @pytest.mark.asyncio
    async def test_low_level_enqueue_wait_release(self, server_port):
        """Low-level: enqueue + wait + release on a free lock."""
        reader, writer = await _open(server_port)
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

    @pytest.mark.asyncio
    async def test_low_level_queued_then_wait(self, server_port):
        """Low-level: conn1 holds, conn2 enqueues (queued), conn1 releases, conn2 waits."""
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
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

    @pytest.mark.asyncio
    async def test_distributed_lock_two_phase(self, server_port):
        """DistributedLock.enqueue() + wait() + release() flow."""
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        status = await lock.enqueue()
        assert status == "acquired"
        assert lock.token is not None

        ok = await lock.wait()
        assert ok is True

        await lock.release()
        assert lock.token is None

    @pytest.mark.asyncio
    async def test_distributed_lock_two_phase_contention(self, server_port):
        """Two DistributedLock instances: lock1 holds, lock2 does enqueue+wait."""
        lock1 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        lock2 = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
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
    @pytest.mark.asyncio
    async def test_acquire_and_release(self, server_port):
        reader, writer = await _open(server_port)
        try:
            token, lease = await aclient.sem_acquire(reader, writer, "s1", 5, 2)
            assert isinstance(token, str) and len(token) > 0
            assert lease > 0
            await aclient.sem_release(reader, writer, "s1", token)
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_acquire_timeout(self, server_port):
        """Semaphore with limit=1 should timeout on the second acquire."""
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
        try:
            await aclient.sem_acquire(r1, w1, "s1", 5, 1)
            with pytest.raises(TimeoutError):
                await aclient.sem_acquire(r2, w2, "s1", 0, 1)
        finally:
            w1.close()
            w2.close()

    @pytest.mark.asyncio
    async def test_release_bad_token(self, server_port):
        reader, writer = await _open(server_port)
        try:
            await aclient.sem_acquire(reader, writer, "s1", 5, 2)
            with pytest.raises(RuntimeError, match="sem_release failed"):
                await aclient.sem_release(reader, writer, "s1", "badtoken")
        finally:
            writer.close()


class TestAsyncSemRenew:
    @pytest.mark.asyncio
    async def test_renew(self, server_port):
        reader, writer = await _open(server_port)
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

    @pytest.mark.asyncio
    async def test_renew_bad_token(self, server_port):
        reader, writer = await _open(server_port)
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
    @pytest.mark.asyncio
    async def test_context_manager(self, server_port):
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
            renew_ratio=0.3,
        )
        async with sem as s:
            assert s.token is not None
        assert sem.token is None

    @pytest.mark.asyncio
    async def test_acquire_release_methods(self, server_port):
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        ok = await sem.acquire()
        assert ok is True
        assert sem.token is not None
        await sem.release()
        assert sem.token is None

    @pytest.mark.asyncio
    async def test_concurrency_within_limit(self, server_port):
        """Semaphore with limit=2 allows 2 concurrent holders but blocks a 3rd."""
        sem1 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        sem2 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        sem3 = aclient.DistributedSemaphore(
            key="sem_conc",
            limit=2,
            acquire_timeout_s=0,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
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
    @pytest.mark.asyncio
    async def test_low_level_enqueue_wait_release(self, server_port):
        reader, writer = await _open(server_port)
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

    @pytest.mark.asyncio
    async def test_low_level_queued_then_wait(self, server_port):
        """conn1 holds (limit=1), conn2 enqueues (queued), conn1 releases, conn2 waits."""
        r1, w1 = await _open(server_port)
        r2, w2 = await _open(server_port)
        try:
            tok1, _ = await aclient.sem_acquire(r1, w1, "s1", 5, 1)

            status, _, _ = await aclient.sem_enqueue(r2, w2, "s1", 1)
            assert status == "queued"

            async def _release_soon():
                await asyncio.sleep(0.1)
                await aclient.sem_release(r1, w1, "s1", tok1)

            release_task = asyncio.create_task(_release_soon())
            tok2, lease2 = await aclient.sem_wait(r2, w2, "s1", 5)
            await release_task

            assert tok2 is not None
            assert lease2 > 0
            await aclient.sem_release(r2, w2, "s1", tok2)
        finally:
            w1.close()
            w2.close()

    @pytest.mark.asyncio
    async def test_distributed_semaphore_two_phase(self, server_port):
        sem = aclient.DistributedSemaphore(
            key="s1",
            limit=2,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        status = await sem.enqueue()
        assert status == "acquired"
        assert sem.token is not None

        ok = await sem.wait()
        assert ok is True

        await sem.release()
        assert sem.token is None

    @pytest.mark.asyncio
    async def test_distributed_semaphore_two_phase_contention(self, server_port):
        """sem1 holds (limit=1), sem2 enqueues+waits."""
        sem1 = aclient.DistributedSemaphore(
            key="s1",
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
        )
        sem2 = aclient.DistributedSemaphore(
            key="s1",
            limit=1,
            acquire_timeout_s=5,
            lease_ttl_s=5,
            servers=[("127.0.0.1", server_port)],
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


class TestAsyncSharding:
    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            aclient.DistributedLock(key="k", servers=[])

    def test_default_servers(self):
        lock = aclient.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    @pytest.mark.asyncio
    async def test_custom_strategy(self, server_port):
        lock = aclient.DistributedLock(
            key="k1",
            acquire_timeout_s=5,
            servers=[("127.0.0.1", server_port)],
            sharding_strategy=lambda key, n: 0,
        )
        async with lock as lk:
            assert lk.token is not None
        assert lock.token is None
