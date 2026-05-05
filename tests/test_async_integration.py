"""Integration tests for the async client against a real dflockd server."""

from __future__ import annotations

import asyncio
import os
import ssl
import uuid

import pytest

import dflockd_client._async as da
from dflockd_client import (
    AsyncConn,
    AsyncDistributedLock,
    AsyncDistributedSemaphore,
)


def _key(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def _open_low_level(host: str, port: int) -> AsyncConn:
    return await da.open_conn(
        host, port, ssl_context=None, connect_timeout_s=5.0
    )


# ===========================================================================
# Low-level: async protocol on AsyncConn
# ===========================================================================


class TestAsyncLowLevelLock:
    async def test_acquire_release_round_trip(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        try:
            key = _key("k")
            tok, lease = await da.acquire(conn, key, 5)
            assert tok and lease > 0
            await da.release(conn, key, tok)
        finally:
            await conn.close()

    async def test_acquire_timeout_when_held(self, server_host_port):
        c1 = await _open_low_level(*server_host_port)
        c2 = await _open_low_level(*server_host_port)
        try:
            key = _key("k")
            await da.acquire(c1, key, 5)
            with pytest.raises(TimeoutError):
                await da.acquire(c2, key, 0)
        finally:
            await c1.close()
            await c2.close()

    async def test_renew_returns_remaining(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        try:
            key = _key("k")
            tok, _ = await da.acquire(conn, key, 5, lease_ttl_s=10)
            assert (await da.renew(conn, key, tok, lease_ttl_s=20)) >= 0
        finally:
            await conn.close()


class TestAsyncTwoPhase:
    async def test_enqueue_acquired_fast_path(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        try:
            key = _key("k")
            status, tok, lease = await da.enqueue(conn, key)
            assert status == "acquired"
            assert tok and lease and lease > 0
            await da.release(conn, key, tok)
        finally:
            await conn.close()

    async def test_queued_then_wait(self, server_host_port):
        host, port = server_host_port
        c1 = await _open_low_level(host, port)
        c2 = await _open_low_level(host, port)
        try:
            key = _key("k")
            tok1, _ = await da.acquire(c1, key, 5)
            status, _, _ = await da.enqueue(c2, key)
            assert status == "queued"

            async def release_soon():
                await asyncio.sleep(0.1)
                await da.release(c1, key, tok1)

            release_task = asyncio.create_task(release_soon())
            tok2, _ = await da.wait(c2, key, 5)
            await release_task
            assert tok2
            await da.release(c2, key, tok2)
        finally:
            await c1.close()
            await c2.close()


class TestAsyncSemaphore:
    async def test_sem_acquire_release(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        try:
            key = _key("s")
            tok, lease = await da.sem_acquire(conn, key, 5, 2)
            assert tok and lease > 0
            await da.sem_release(conn, key, tok)
        finally:
            await conn.close()

    async def test_sem_third_blocks(self, server_host_port):
        host, port = server_host_port
        conns = [await _open_low_level(host, port) for _ in range(3)]
        try:
            key = _key("s")
            await da.sem_acquire(conns[0], key, 5, 2)
            await da.sem_acquire(conns[1], key, 5, 2)
            with pytest.raises(TimeoutError):
                await da.sem_acquire(conns[2], key, 0, 2)
        finally:
            for c in conns:
                await c.close()


class TestAsyncStats:
    async def test_decoded_shape(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        try:
            result = await da.stats(conn)
            assert {"connections", "locks", "semaphores",
                    "idle_locks", "idle_semaphores"}.issubset(result.keys())
        finally:
            await conn.close()


# ===========================================================================
# High-level: AsyncDistributedLock
# ===========================================================================


class TestAsyncDistributedLock:
    async def test_context_manager(self, server_host_port):
        host, port = server_host_port
        lock = AsyncDistributedLock(
            key=_key("ctx"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        async with lock as held:
            assert held.token is not None
        assert lock.token is None

    async def test_acquire_release_methods(self, server_host_port):
        host, port = server_host_port
        lock = AsyncDistributedLock(
            key=_key("acq"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        assert await lock.acquire() is True
        assert lock.token is not None
        await lock.release()
        assert lock.token is None

    async def test_acquire_timeout_returns_false(self, server_host_port):
        host, port = server_host_port
        key = _key("timeout")
        holder = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=30,
            servers=[(host, port)],
        )
        contender = AsyncDistributedLock(
            key=key, acquire_timeout_s=0, lease_ttl_s=30,
            servers=[(host, port)],
        )
        await holder.acquire()
        try:
            assert await contender.acquire() is False
        finally:
            await holder.release()

    async def test_mutual_exclusion(self, server_host_port):
        host, port = server_host_port
        key = _key("mutex")
        results: list[int] = []

        async def worker(n: int):
            inst = AsyncDistributedLock(
                key=key, acquire_timeout_s=5, lease_ttl_s=5,
                servers=[(host, port)],
            )
            async with inst:
                results.append(n)
                await asyncio.sleep(0.05)

        t1 = asyncio.create_task(worker(1))
        await asyncio.sleep(0.02)
        t2 = asyncio.create_task(worker(2))
        await asyncio.gather(t1, t2)
        assert results == [1, 2]

    async def test_two_phase_acquired(self, server_host_port):
        host, port = server_host_port
        lock = AsyncDistributedLock(
            key=_key("two"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        try:
            assert await lock.enqueue() == "acquired"
            assert await lock.wait() is True
        finally:
            await lock.release()

    async def test_two_phase_queued_then_granted(self, server_host_port):
        host, port = server_host_port
        key = _key("queued")
        l1 = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        l2 = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        await l1.acquire()
        try:
            assert await l2.enqueue() == "queued"

            async def release_soon():
                await asyncio.sleep(0.1)
                await l1.release()

            t = asyncio.create_task(release_soon())
            assert await l2.wait() is True
            await t
        finally:
            await l2.release()

    async def test_fifo_ordering(self, server_host_port):
        host, port = server_host_port
        key = _key("fifo")
        N = 5
        holder = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=10,
            servers=[(host, port)],
        )
        await holder.acquire()
        waiters = [
            AsyncDistributedLock(
                key=key, acquire_timeout_s=10, lease_ttl_s=10,
                servers=[(host, port)],
            )
            for _ in range(N)
        ]
        for w in waiters:
            assert await w.enqueue() == "queued"
            await asyncio.sleep(0.05)
        await holder.release()
        order: list[int] = []

        async def runner(idx: int, w: AsyncDistributedLock):
            assert await w.wait() is True
            order.append(idx)
            await w.release()

        await asyncio.gather(*(runner(i, w) for i, w in enumerate(waiters)))
        assert order == list(range(N))

    async def test_disconnect_releases_lock(self, server_host_port):
        host, port = server_host_port
        key = _key("disc")
        first = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=30,
            servers=[(host, port)],
        )
        await first.acquire()
        await first.aclose()
        await asyncio.sleep(0.2)
        second = AsyncDistributedLock(
            key=key, acquire_timeout_s=1, lease_ttl_s=30,
            servers=[(host, port)],
        )
        try:
            assert await second.acquire() is True
        finally:
            await second.release()


class TestAsyncDistributedSemaphore:
    async def test_acquire_release(self, server_host_port):
        host, port = server_host_port
        sem = AsyncDistributedSemaphore(
            key=_key("s"), limit=2, acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        assert await sem.acquire() is True
        await sem.release()

    async def test_third_blocks(self, server_host_port):
        host, port = server_host_port
        key = _key("s")
        s1 = AsyncDistributedSemaphore(
            key=key, limit=2, acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        s2 = AsyncDistributedSemaphore(
            key=key, limit=2, acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        s3 = AsyncDistributedSemaphore(
            key=key, limit=2, acquire_timeout_s=0, lease_ttl_s=5,
            servers=[(host, port)],
        )
        await s1.acquire()
        await s2.acquire()
        try:
            assert await s3.acquire() is False
        finally:
            await s1.release()
            await s2.release()


# ===========================================================================
# Optional: TLS / auth integration
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


class TestAsyncRenewLoop:
    async def test_lock_survives_past_initial_lease(self, server_host_port):
        """If renewal silently breaks, the server-side lease lapses and a
        contender can grab the key. With renewal working, the lock stays
        held past the original lease window."""
        host, port = server_host_port
        key = _key("renew-survives")
        holder = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=2, renew_ratio=0.5,
            servers=[(host, port)],
        )
        await holder.acquire()
        try:
            await asyncio.sleep(4.0)
            contender = AsyncDistributedLock(
                key=key, acquire_timeout_s=0, lease_ttl_s=5,
                servers=[(host, port)],
            )
            assert await contender.acquire() is False, "renew failed — lease lapsed"
            assert holder.token is not None
        finally:
            await holder.release()


class TestAsyncInstanceReuse:
    async def test_acquire_release_acquire_round_trip(self, server_host_port):
        host, port = server_host_port
        lock = AsyncDistributedLock(
            key=_key("reuse"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[(host, port)],
        )
        for _ in range(3):
            assert await lock.acquire() is True
            assert lock.token is not None
            assert await lock.release() is True
            assert lock.token is None

    async def test_acquire_after_acquire_silently_resets(self, server_host_port):
        host, port = server_host_port
        lock = AsyncDistributedLock(
            key=_key("reset"), acquire_timeout_s=5, lease_ttl_s=30,
            servers=[(host, port)],
        )
        await lock.acquire()
        first = lock.token
        try:
            await lock.acquire()
            second = lock.token
            assert second is not None
            assert second != first
        finally:
            await lock.release()


class TestAsyncContextManagerCleanup:
    async def test_releases_on_exception_inside_block(self, server_host_port):
        host, port = server_host_port
        key = _key("ctx-exc")
        lock = AsyncDistributedLock(
            key=key, acquire_timeout_s=5, lease_ttl_s=30,
            servers=[(host, port)],
        )

        class Boom(Exception):
            pass

        with pytest.raises(Boom):
            async with lock:
                raise Boom()

        assert lock.token is None
        contender = AsyncDistributedLock(
            key=key, acquire_timeout_s=2, lease_ttl_s=5,
            servers=[(host, port)],
        )
        try:
            assert await contender.acquire() is True
        finally:
            await contender.release()


class TestAsyncConnConcurrentUse:
    """Two coroutines calling ``acquire`` on different keys against one
    ``AsyncConn`` must not interleave bytes on the wire."""

    async def test_two_tasks_share_one_conn(self, server_host_port):
        host, port = server_host_port
        conn = await _open_low_level(host, port)
        results: dict[str, str] = {}

        async def worker(k: str):
            tok, _ = await da.acquire(conn, k, 5, lease_ttl_s=10)
            results[k] = tok
            await da.release(conn, k, tok)

        try:
            keys = [_key(f"shared-async-{i}") for i in range(8)]
            await asyncio.gather(*(worker(k) for k in keys))
            assert len(results) == len(keys)
            assert len(set(results.values())) == len(keys)
        finally:
            await conn.close()


class TestAsyncStatsWithManyLocks:
    """Regression: ``stats`` JSON exceeds 64 KiB for moderate working sets;
    the asyncio ``StreamReader`` must accept lines up to the documented
    response cap, not asyncio's 64 KiB default."""

    async def test_stats_handles_kilobyte_scale(self, server_host_port):
        host, port = server_host_port
        holders = [await _open_low_level(host, port) for _ in range(200)]
        tokens: list[tuple[str, str]] = []
        try:
            for i, c in enumerate(holders):
                k = f"manylocks-async-{uuid.uuid4().hex[:8]}-{i}"
                tok, _ = await da.acquire(c, k, 5, lease_ttl_s=30)
                tokens.append((k, tok))
            stats_conn = await _open_low_level(host, port)
            try:
                result = await da.stats(stats_conn)
                assert len(result["locks"]) >= 200
            finally:
                await stats_conn.close()
        finally:
            for c, (k, tok) in zip(holders, tokens):
                try:
                    await da.release(c, k, tok)
                except Exception:
                    pass
                await c.close()


@_skip_tls
class TestAsyncTlsIntegration:
    async def test_lock_over_tls(self):
        port = int(os.environ["DFLOCKD_TEST_TLS_PORT"])
        ctx = ssl.create_default_context(
            cafile=os.environ.get("DFLOCKD_TEST_TLS_CA")
        )
        lock = AsyncDistributedLock(
            key=_key("tls"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[("127.0.0.1", port)], ssl_context=ctx,
        )
        async with lock as held:
            assert held.token is not None


@_skip_auth
class TestAsyncAuthIntegration:
    async def test_lock_with_auth(self):
        port = int(os.environ["DFLOCKD_TEST_AUTH_PORT"])
        token = os.environ["DFLOCKD_TEST_AUTH_TOKEN"]
        lock = AsyncDistributedLock(
            key=_key("auth"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[("127.0.0.1", port)], auth_token=token,
        )
        async with lock as held:
            assert held.token is not None

    async def test_bad_token_rejected(self):
        port = int(os.environ["DFLOCKD_TEST_AUTH_PORT"])
        lock = AsyncDistributedLock(
            key=_key("auth"), acquire_timeout_s=5, lease_ttl_s=5,
            servers=[("127.0.0.1", port)], auth_token="wrong-token",
        )
        with pytest.raises(PermissionError, match="authentication failed"):
            await lock.acquire()
