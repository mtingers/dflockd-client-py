"""Tests for dflockd_client.sync_client — unit tests and integration tests."""

import asyncio
import io
import socket
import ssl
import threading
import time

import pytest

import dflockd_client.sync_client as sc


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _connect(port: int) -> tuple[socket.socket, io.TextIOWrapper]:
    sock = socket.create_connection(("127.0.0.1", port))
    rfile = sock.makefile("r", encoding="utf-8")
    return sock, rfile


def _close(sock: socket.socket, rfile: io.TextIOWrapper) -> None:
    rfile.close()
    sock.close()


# ===========================================================================
# Unit tests — no server needed
# ===========================================================================


class TestEncodeLines:
    def test_single_line(self):
        assert sc._encode_lines("hello") == b"hello\n"

    def test_multiple_lines(self):
        assert sc._encode_lines("a", "b", "c") == b"a\nb\nc\n"

    def test_empty_string(self):
        assert sc._encode_lines("") == b"\n"

    def test_unicode(self):
        result = sc._encode_lines("caf\u00e9")
        assert result == "caf\u00e9\n".encode("utf-8")


class TestReadline:
    def test_normal(self):
        buf = io.StringIO("hello\n")
        assert sc._readline(buf) == "hello"

    def test_strips_crlf(self):
        buf = io.StringIO("hello\r\n")
        assert sc._readline(buf) == "hello"

    def test_eof_raises(self):
        buf = io.StringIO("")
        with pytest.raises(ConnectionError, match="server closed"):
            sc._readline(buf)

    def test_multiple_reads(self):
        buf = io.StringIO("line1\nline2\n")
        assert sc._readline(buf) == "line1"
        assert sc._readline(buf) == "line2"


class TestDistributedLockDefaults:
    def test_default_fields(self):
        lock = sc.DistributedLock(key="mykey")
        assert lock.key == "mykey"
        assert lock.acquire_timeout_s == 10
        assert lock.lease_ttl_s is None
        assert lock.servers == [("127.0.0.1", 6388)]
        assert lock.renew_ratio == 0.5
        assert lock.ssl_context is None
        assert lock.token is None
        assert lock.lease == 0
        assert lock._closed is False

    def test_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        lock = sc.DistributedLock(key="k", ssl_context=ctx)
        assert lock.ssl_context is ctx

    def test_close_idempotent(self):
        lock = sc.DistributedLock(key="k")
        lock.close()
        lock.close()  # second call should be a no-op


# ===========================================================================
# Integration tests — low-level functions
# ===========================================================================


class TestAcquireRelease:
    async def test_basic_cycle(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, lease = sc.acquire(sock, rfile, "k1", 5)
                assert isinstance(token, str) and len(token) > 0
                assert lease > 0
                sc.release(sock, rfile, "k1", token)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_acquire_with_custom_lease(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, lease = sc.acquire(sock, rfile, "k1", 5, lease_ttl_s=20)
                assert lease == 20
                sc.release(sock, rfile, "k1", token)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_acquire_timeout(self, server_port):
        """Second acquire on same key with timeout=0 should raise TimeoutError."""

        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                sc.acquire(s1, r1, "k1", 5)
                with pytest.raises(TimeoutError):
                    sc.acquire(s2, r2, "k1", 0)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)

    async def test_release_bad_token(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                sc.acquire(sock, rfile, "k1", 5)
                with pytest.raises(RuntimeError, match="release failed"):
                    sc.release(sock, rfile, "k1", "badtoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_release_nonexistent_key(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                with pytest.raises(RuntimeError, match="release failed"):
                    sc.release(sock, rfile, "nokey", "notoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_multiple_keys(self, server_port):
        """Acquire different keys on the same connection sequentially."""

        def _work():
            sock, rfile = _connect(server_port)
            try:
                t1, _ = sc.acquire(sock, rfile, "k1", 5)
                t2, _ = sc.acquire(sock, rfile, "k2", 5)
                assert t1 != t2
                sc.release(sock, rfile, "k1", t1)
                sc.release(sock, rfile, "k2", t2)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)


class TestRenew:
    async def test_renew_returns_remaining(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, _ = sc.acquire(sock, rfile, "k1", 5, lease_ttl_s=10)
                remaining = sc.renew(sock, rfile, "k1", token, lease_ttl_s=20)
                assert remaining >= 0
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_renew_default_lease(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, _ = sc.acquire(sock, rfile, "k1", 5)
                remaining = sc.renew(sock, rfile, "k1", token)
                assert remaining >= 0
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_renew_bad_token(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                sc.acquire(sock, rfile, "k1", 5)
                with pytest.raises(RuntimeError, match="renew failed"):
                    sc.renew(sock, rfile, "k1", "badtoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_renew_nonexistent_key(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                with pytest.raises(RuntimeError, match="renew failed"):
                    sc.renew(sock, rfile, "nokey", "notoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)


# ===========================================================================
# DistributedLock — context manager (__enter__ / __exit__)
# ===========================================================================


class TestDistributedLockContextManager:
    async def test_basic_lifecycle(self, server_port):
        def _work():
            lock = sc.DistributedLock(
                key="k1", acquire_timeout_s=5, servers=[("127.0.0.1", server_port)]
            )
            with lock as lk:
                assert lk is lock
                assert lk.token is not None
                assert lk.lease > 0
            # After exit, token is cleared
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_custom_lease(self, server_port):
        def _work():
            lock = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=15,
                servers=[("127.0.0.1", server_port)],
            )
            with lock:
                assert lock.lease == 15

        await asyncio.to_thread(_work)

    async def test_exception_inside_context(self, server_port):
        """Lock is released even if an exception is raised inside the block."""

        def _work():
            lock = sc.DistributedLock(
                key="k1", acquire_timeout_s=5, servers=[("127.0.0.1", server_port)]
            )
            with pytest.raises(ValueError, match="boom"):
                with lock:
                    assert lock.token is not None
                    raise ValueError("boom")
            assert lock.token is None
            assert lock._closed is True

        await asyncio.to_thread(_work)

    async def test_reuse_after_exit(self, server_port):
        """A lock object can be used again after exiting its context."""

        def _work():
            lock = sc.DistributedLock(
                key="k1", acquire_timeout_s=5, servers=[("127.0.0.1", server_port)]
            )
            with lock:
                token1 = lock.token
            with lock:
                token2 = lock.token
            assert token1 != token2

        await asyncio.to_thread(_work)


# ===========================================================================
# DistributedLock — acquire() / release() methods
# ===========================================================================


class TestDistributedLockMethods:
    async def test_acquire_release(self, server_port):
        def _work():
            lock = sc.DistributedLock(
                key="k1", acquire_timeout_s=5, servers=[("127.0.0.1", server_port)]
            )
            ok = lock.acquire()
            assert ok is True
            assert lock.token is not None
            ok = lock.release()
            assert ok is True
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_acquire_timeout_returns_false(self, server_port):
        def _work():
            lock1 = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=30,
                servers=[("127.0.0.1", server_port)],
            )
            lock2 = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=0,
                lease_ttl_s=30,
                servers=[("127.0.0.1", server_port)],
            )
            lock1.acquire()
            try:
                ok = lock2.acquire()
                assert ok is False
                assert lock2.token is None
            finally:
                lock1.release()

        await asyncio.to_thread(_work)

    async def test_close_after_acquire(self, server_port):
        """Calling close() directly (without release) still cleans up."""

        def _work():
            lock = sc.DistributedLock(
                key="k1", acquire_timeout_s=5, servers=[("127.0.0.1", server_port)]
            )
            lock.acquire()
            assert lock.token is not None
            lock.close()
            assert lock.token is None
            assert lock._sock is None
            assert lock._rfile is None

        await asyncio.to_thread(_work)


# ===========================================================================
# Renew loop
# ===========================================================================


class TestRenewLoop:
    async def test_renew_keeps_lock_alive(self, server_port):
        """With a short lease and renew, the lock should stay held past its initial TTL."""

        def _work():
            lock = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=2,
                servers=[("127.0.0.1", server_port)],
                renew_ratio=0.3,  # renew every ~0.6s
            )
            with lock:
                # Sleep past original lease
                time.sleep(3)
                # Lock should still be held because renew kept it alive
                assert lock.token is not None

        await asyncio.to_thread(_work)

    async def test_renew_stops_on_release(self, server_port):
        """After release(), the renew thread should be stopped."""

        def _work():
            lock = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
                renew_ratio=0.3,
            )
            lock.acquire()
            assert lock._renew_thread is not None
            assert lock._renew_thread.is_alive()
            lock.release()
            assert lock._renew_thread is None

        await asyncio.to_thread(_work)


# ===========================================================================
# Mutual exclusion
# ===========================================================================


class TestMutualExclusion:
    async def test_two_threads_serial(self, server_port):
        """Two threads competing for the same key execute their critical
        sections sequentially (no overlap)."""
        results: list[tuple[int, str]] = []
        results_lock = threading.Lock()

        def _worker(n: int):
            lock = sc.DistributedLock(
                key="mutex",
                acquire_timeout_s=10,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with lock:
                with results_lock:
                    results.append((n, "enter"))
                time.sleep(0.15)
                with results_lock:
                    results.append((n, "exit"))

        def _run():
            t1 = threading.Thread(target=_worker, args=(1,))
            t2 = threading.Thread(target=_worker, args=(2,))
            t1.start()
            time.sleep(0.02)  # give t1 a head start
            t2.start()
            t1.join(timeout=15)
            t2.join(timeout=15)

        await asyncio.to_thread(_run)

        # Verify non-overlapping: first worker must exit before second enters
        assert len(results) == 4
        assert results[0] == (1, "enter")
        assert results[1] == (1, "exit")
        assert results[2] == (2, "enter")
        assert results[3] == (2, "exit")

    async def test_different_keys_concurrent(self, server_port):
        """Different keys can be held simultaneously."""
        barrier = threading.Barrier(2, timeout=10)
        held = [False, False]

        def _worker(n: int, key: str):
            lock = sc.DistributedLock(
                key=key,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with lock:
                held[n] = True
                barrier.wait()  # both threads must reach here simultaneously
                assert held[0] and held[1]

        def _run():
            t1 = threading.Thread(target=_worker, args=(0, "key_a"))
            t2 = threading.Thread(target=_worker, args=(1, "key_b"))
            t1.start()
            t2.start()
            t1.join(timeout=15)
            t2.join(timeout=15)

        await asyncio.to_thread(_run)


# ===========================================================================
# Disconnect behavior
# ===========================================================================


class TestDisconnect:
    async def test_abrupt_close_frees_lock(self, server_port):
        """Closing the socket without releasing lets the server clean up."""

        def _work():
            s1, r1 = _connect(server_port)
            sc.acquire(s1, r1, "k1", 5)
            # Close abruptly without release
            r1.close()
            s1.close()
            # Small delay for server to notice the disconnect
            time.sleep(0.3)

            # A new client should be able to acquire
            s2, r2 = _connect(server_port)
            try:
                token, _ = sc.acquire(s2, r2, "k1", 2)
                assert token is not None
            finally:
                _close(s2, r2)

        await asyncio.to_thread(_work)

    def test_server_closed_connection_raises(self):
        """If the server side closes, _readline should raise ConnectionError."""
        buf = io.StringIO("")
        with pytest.raises(ConnectionError):
            sc._readline(buf)


# ===========================================================================
# Sync two-phase: enqueue + wait
# ===========================================================================


class TestSyncTwoPhase:
    async def test_low_level_enqueue_wait_release(self, server_port):
        """Low-level: enqueue + wait + release on a free lock."""

        def _work():
            sock, rfile = _connect(server_port)
            try:
                status, token, lease = sc.enqueue(sock, rfile, "k1")
                assert status == "acquired"
                assert token is not None
                assert lease > 0

                tok, ttl = sc.wait(sock, rfile, "k1", 5)
                assert tok == token

                sc.release(sock, rfile, "k1", tok)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_queued_then_wait(self, server_port):
        """conn1 holds lock, conn2 enqueues (queued), conn1 releases, conn2 waits."""

        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                tok1, _ = sc.acquire(s1, r1, "k1", 5)

                status, _, _ = sc.enqueue(s2, r2, "k1")
                assert status == "queued"

                # Release in a thread so conn2 can wait
                def _release():
                    time.sleep(0.1)
                    sc.release(s1, r1, "k1", tok1)

                t = threading.Thread(target=_release)
                t.start()
                tok2, lease2 = sc.wait(s2, r2, "k1", 5)
                t.join()

                assert tok2 is not None
                assert lease2 > 0
                sc.release(s2, r2, "k1", tok2)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)

    async def test_distributed_lock_two_phase(self, server_port):
        """DistributedLock.enqueue() + wait() + release() flow."""

        def _work():
            lock = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            status = lock.enqueue()
            assert status == "acquired"
            assert lock.token is not None

            ok = lock.wait()
            assert ok is True

            lock.release()
            assert lock.token is None

        await asyncio.to_thread(_work)

    async def test_distributed_lock_two_phase_contention(self, server_port):
        """lock1 holds, lock2 does enqueue+wait, lock1 releases -> lock2 gets it."""

        def _work():
            lock1 = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            lock2 = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )

            lock1.acquire()
            status = lock2.enqueue()
            assert status == "queued"

            def _release():
                time.sleep(0.1)
                lock1.release()

            t = threading.Thread(target=_release)
            t.start()
            ok = lock2.wait()
            t.join()

            assert ok is True
            assert lock2.token is not None
            lock2.release()

        await asyncio.to_thread(_work)


# ===========================================================================
# Semaphore — unit tests (no server needed)
# ===========================================================================


class TestDistributedSemaphoreDefaults:
    def test_default_fields(self):
        sem = sc.DistributedSemaphore(key="mykey", limit=3)
        assert sem.key == "mykey"
        assert sem.limit == 3
        assert sem.acquire_timeout_s == 10
        assert sem.lease_ttl_s is None
        assert sem.servers == [("127.0.0.1", 6388)]
        assert sem.renew_ratio == 0.5
        assert sem.ssl_context is None
        assert sem.token is None
        assert sem.lease == 0
        assert sem._closed is False

    def test_ssl_context_set(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        sem = sc.DistributedSemaphore(key="k", limit=2, ssl_context=ctx)
        assert sem.ssl_context is ctx

    def test_close_idempotent(self):
        sem = sc.DistributedSemaphore(key="k", limit=2)
        sem.close()
        sem.close()  # second call should be a no-op


# ===========================================================================
# Semaphore — integration tests: low-level functions
# ===========================================================================


class TestSemAcquireRelease:
    async def test_basic_cycle(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, lease = sc.sem_acquire(sock, rfile, "s1", 5, 2)
                assert isinstance(token, str) and len(token) > 0
                assert lease > 0
                sc.sem_release(sock, rfile, "s1", token)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_acquire_timeout(self, server_port):
        """Semaphore with limit=1 should timeout on second acquire."""

        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                sc.sem_acquire(s1, r1, "s1_lim1", 5, 1)
                with pytest.raises(TimeoutError):
                    sc.sem_acquire(s2, r2, "s1_lim1", 0, 1)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)

    async def test_release_bad_token(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                sc.sem_acquire(sock, rfile, "s1", 5, 2)
                with pytest.raises(RuntimeError, match="sem_release failed"):
                    sc.sem_release(sock, rfile, "s1", "badtoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)


class TestSemRenew:
    async def test_renew_returns_remaining(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                token, _ = sc.sem_acquire(sock, rfile, "s1", 5, 2, lease_ttl_s=10)
                remaining = sc.sem_renew(sock, rfile, "s1", token, lease_ttl_s=20)
                assert remaining >= 0
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_renew_bad_token(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                sc.sem_acquire(sock, rfile, "s1", 5, 2)
                with pytest.raises(RuntimeError, match="sem_renew failed"):
                    sc.sem_renew(sock, rfile, "s1", "badtoken")
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)


# ===========================================================================
# DistributedSemaphore — context manager
# ===========================================================================


class TestDistributedSemaphoreContextManager:
    async def test_basic_lifecycle(self, server_port):
        def _work():
            sem = sc.DistributedSemaphore(
                key="s1",
                limit=2,
                acquire_timeout_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with sem as s:
                assert s is sem
                assert s.token is not None
                assert s.lease > 0
            assert sem.token is None

        await asyncio.to_thread(_work)

    async def test_exception_inside_context(self, server_port):
        def _work():
            sem = sc.DistributedSemaphore(
                key="s1",
                limit=2,
                acquire_timeout_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with pytest.raises(ValueError, match="boom"):
                with sem:
                    assert sem.token is not None
                    raise ValueError("boom")
            assert sem.token is None
            assert sem._closed is True

        await asyncio.to_thread(_work)

    async def test_reuse_after_exit(self, server_port):
        def _work():
            sem = sc.DistributedSemaphore(
                key="s1",
                limit=2,
                acquire_timeout_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with sem:
                token1 = sem.token
            with sem:
                token2 = sem.token
            assert token1 != token2

        await asyncio.to_thread(_work)


# ===========================================================================
# DistributedSemaphore — acquire() / release() methods
# ===========================================================================


class TestDistributedSemaphoreMethods:
    async def test_acquire_release(self, server_port):
        def _work():
            sem = sc.DistributedSemaphore(
                key="s1",
                limit=2,
                acquire_timeout_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            ok = sem.acquire()
            assert ok is True
            assert sem.token is not None
            ok = sem.release()
            assert ok is True
            assert sem.token is None

        await asyncio.to_thread(_work)

    async def test_acquire_timeout_returns_false(self, server_port):
        def _work():
            sem1 = sc.DistributedSemaphore(
                key="s1_lim1",
                limit=1,
                acquire_timeout_s=5,
                lease_ttl_s=30,
                servers=[("127.0.0.1", server_port)],
            )
            sem2 = sc.DistributedSemaphore(
                key="s1_lim1",
                limit=1,
                acquire_timeout_s=0,
                lease_ttl_s=30,
                servers=[("127.0.0.1", server_port)],
            )
            sem1.acquire()
            try:
                ok = sem2.acquire()
                assert ok is False
                assert sem2.token is None
            finally:
                sem1.release()

        await asyncio.to_thread(_work)


# ===========================================================================
# Semaphore mutual exclusion (concurrency limit)
# ===========================================================================


class TestSemMutualExclusion:
    async def test_limit_allows_n_blocks_n_plus_1(self, server_port):
        """Semaphore with limit=2 allows 2 concurrent holders but blocks a 3rd."""
        results: list[tuple[int, str]] = []
        results_lock = threading.Lock()

        def _worker(n: int, timeout: int):
            sem = sc.DistributedSemaphore(
                key="sem_mutex",
                limit=2,
                acquire_timeout_s=timeout,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            with sem:
                with results_lock:
                    results.append((n, "enter"))
                time.sleep(0.15)
                with results_lock:
                    results.append((n, "exit"))

        def _run():
            t1 = threading.Thread(target=_worker, args=(1, 5))
            t2 = threading.Thread(target=_worker, args=(2, 5))
            t3 = threading.Thread(target=_worker, args=(3, 5))
            t1.start()
            t2.start()
            time.sleep(0.05)  # let t1 and t2 acquire
            t3.start()
            t1.join(timeout=15)
            t2.join(timeout=15)
            t3.join(timeout=15)

        await asyncio.to_thread(_run)

        # Workers 1 and 2 should both enter before either exits
        # Worker 3 should enter only after one of them exits
        enter_order = [r[0] for r in results if r[1] == "enter"]
        assert len(enter_order) == 3
        # First two entered concurrently
        assert set(enter_order[:2]) == {1, 2}
        # Third entered after at least one exit
        first_exit_idx = next(i for i, r in enumerate(results) if r[1] == "exit")
        third_enter_idx = next(i for i, r in enumerate(results) if r == (3, "enter"))
        assert third_enter_idx > first_exit_idx


# ===========================================================================
# Sync semaphore two-phase: enqueue + wait
# ===========================================================================


class TestSyncSemTwoPhase:
    async def test_low_level_enqueue_wait_release(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                status, token, lease = sc.sem_enqueue(sock, rfile, "s1", 2)
                assert status == "acquired"
                assert token is not None
                assert lease > 0

                tok, ttl = sc.sem_wait(sock, rfile, "s1", 5)
                assert tok == token

                sc.sem_release(sock, rfile, "s1", tok)
            finally:
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_queued_then_wait(self, server_port):
        """conn1 holds (limit=1), conn2 enqueues (queued), conn1 releases, conn2 waits."""

        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                tok1, _ = sc.sem_acquire(s1, r1, "s1_queue", 5, 1)

                status, _, _ = sc.sem_enqueue(s2, r2, "s1_queue", 1)
                assert status == "queued"

                def _release():
                    time.sleep(0.1)
                    sc.sem_release(s1, r1, "s1_queue", tok1)

                t = threading.Thread(target=_release)
                t.start()
                tok2, lease2 = sc.sem_wait(s2, r2, "s1_queue", 5)
                t.join()

                assert tok2 is not None
                assert lease2 > 0
                sc.sem_release(s2, r2, "s1_queue", tok2)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)

    async def test_distributed_semaphore_two_phase(self, server_port):
        def _work():
            sem = sc.DistributedSemaphore(
                key="s1",
                limit=2,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            status = sem.enqueue()
            assert status == "acquired"
            assert sem.token is not None

            ok = sem.wait()
            assert ok is True

            sem.release()
            assert sem.token is None

        await asyncio.to_thread(_work)

    async def test_distributed_semaphore_two_phase_contention(self, server_port):
        """sem1 holds (limit=1), sem2 enqueues+waits, sem1 releases -> sem2 gets it."""

        def _work():
            sem1 = sc.DistributedSemaphore(
                key="s1_2phase",
                limit=1,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )
            sem2 = sc.DistributedSemaphore(
                key="s1_2phase",
                limit=1,
                acquire_timeout_s=5,
                lease_ttl_s=5,
                servers=[("127.0.0.1", server_port)],
            )

            sem1.acquire()
            status = sem2.enqueue()
            assert status == "queued"

            def _release():
                time.sleep(0.1)
                sem1.release()

            t = threading.Thread(target=_release)
            t.start()
            ok = sem2.wait()
            t.join()

            assert ok is True
            assert sem2.token is not None
            sem2.release()

        await asyncio.to_thread(_work)


# ===========================================================================
# Sharding
# ===========================================================================


# ===========================================================================
# Stats
# ===========================================================================


class TestStats:
    async def test_stats_returns_dict(self, server_port):
        def _work():
            sock, rfile = _connect(server_port)
            try:
                result = sc.stats(sock, rfile)
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
                _close(sock, rfile)

        await asyncio.to_thread(_work)

    async def test_stats_shows_held_lock(self, server_port):
        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                token, _ = sc.acquire(s1, r1, "stats_lock", 5, lease_ttl_s=30)
                result = sc.stats(s2, r2)
                lock_keys = [lk["key"] for lk in result["locks"]]
                assert "stats_lock" in lock_keys
                sc.release(s1, r1, "stats_lock", token)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)

    async def test_stats_shows_held_semaphore(self, server_port):
        def _work():
            s1, r1 = _connect(server_port)
            s2, r2 = _connect(server_port)
            try:
                token, _ = sc.sem_acquire(s1, r1, "stats_sem", 5, 2, lease_ttl_s=30)
                result = sc.stats(s2, r2)
                sem_keys = [s["key"] for s in result["semaphores"]]
                assert "stats_sem" in sem_keys
                sc.sem_release(s1, r1, "stats_sem", token)
            finally:
                _close(s1, r1)
                _close(s2, r2)

        await asyncio.to_thread(_work)


class TestSharding:
    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            sc.DistributedLock(key="k", servers=[])

    def test_default_servers(self):
        lock = sc.DistributedLock(key="k")
        assert lock.servers == [("127.0.0.1", 6388)]

    async def test_custom_strategy(self, server_port):
        def _work():
            lock = sc.DistributedLock(
                key="k1",
                acquire_timeout_s=5,
                servers=[("127.0.0.1", server_port)],
                sharding_strategy=lambda key, n: 0,
            )
            with lock as lk:
                assert lk.token is not None
            assert lock.token is None

        await asyncio.to_thread(_work)
