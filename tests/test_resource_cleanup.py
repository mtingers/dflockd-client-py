"""Unit tests for resource cleanup: __del__ FD release, wait_closed timeout,
and renew-loop local reference cleanup.

These tests use mocking/fakes — no dflockd server required.
"""

import asyncio
import gc
import io
import socket
import threading
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dflockd_client.client as aclient
import dflockd_client.sync_client as sclient


# ===========================================================================
# Helpers
# ===========================================================================


def _make_sync_lock(**overrides):
    """Create a SyncDistributedLock with safe defaults (no real server)."""
    defaults = dict(key="test-key", servers=[("127.0.0.1", 9999)])
    defaults.update(overrides)
    return sclient.DistributedLock(**defaults)


def _make_async_lock(**overrides):
    """Create an AsyncDistributedLock with safe defaults (no real server)."""
    defaults = dict(key="test-key", servers=[("127.0.0.1", 9999)])
    defaults.update(overrides)
    return aclient.DistributedLock(**defaults)


# ===========================================================================
# Sync __del__ closes the socket FD
# ===========================================================================


class TestSyncDelClosesSocket:
    def test_del_closes_socket_on_gc(self):
        """When a sync lock is GC'd without close(), __del__ should close the socket."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        lock._sock = mock_sock
        lock._closed = False

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
            lock._sock = None  # prevent duplicate warning on test teardown

        mock_sock.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1
        assert "garbage collected" in str(resource_warnings[0].message)

    def test_del_no_warning_when_already_closed(self):
        """No warning or close when _sock is None (already cleaned up)."""
        lock = _make_sync_lock()
        assert lock._sock is None

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()

        assert len(w) == 0

    def test_del_tolerates_close_exception(self):
        """If sock.close() raises, __del__ should not propagate."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.close.side_effect = OSError("already closed")
        lock._sock = mock_sock

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            lock.__del__()  # should not raise
            lock._sock = None

        mock_sock.close.assert_called_once()

    def test_del_on_semaphore(self):
        """DistributedSemaphore.__del__ also closes the socket."""
        sem = sclient.DistributedSemaphore(
            key="test-key", limit=2, servers=[("127.0.0.1", 9999)]
        )
        mock_sock = MagicMock(spec=socket.socket)
        sem._sock = mock_sock

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sem.__del__()
            sem._sock = None  # prevent duplicate warning on test teardown

        mock_sock.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1

    def test_actual_gc_closes_fd(self):
        """Simulate real GC: create a lock, stuff a fake socket, drop all refs."""
        mock_sock = MagicMock(spec=socket.socket)
        lock = _make_sync_lock()
        lock._sock = mock_sock
        lock._closed = False
        # Drop all references — GC should trigger __del__ which calls close()
        del lock
        gc.collect()

        mock_sock.close.assert_called_once()


# ===========================================================================
# Async __del__ closes the writer/transport
# ===========================================================================


class TestAsyncDelClosesWriter:
    def test_del_closes_writer_on_gc(self):
        """When an async lock is GC'd without aclose(), __del__ should close the writer."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        lock._writer = mock_writer

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()
            lock._writer = None

        mock_writer.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1
        assert "garbage collected" in str(resource_warnings[0].message)

    def test_del_no_warning_when_already_closed(self):
        """No warning or close when _writer is None (already cleaned up)."""
        lock = _make_async_lock()
        assert lock._writer is None

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            lock.__del__()

        assert len(w) == 0

    def test_del_tolerates_close_exception(self):
        """If writer.close() raises, __del__ should not propagate."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close.side_effect = RuntimeError("already closed")
        lock._writer = mock_writer

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            lock.__del__()  # should not raise
            lock._writer = None

        mock_writer.close.assert_called_once()

    def test_del_on_semaphore(self):
        """AsyncDistributedSemaphore.__del__ also closes the writer."""
        sem = aclient.DistributedSemaphore(
            key="test-key", limit=2, servers=[("127.0.0.1", 9999)]
        )
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        sem._writer = mock_writer

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sem.__del__()
            sem._writer = None

        mock_writer.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1

    def test_actual_gc_closes_writer(self):
        """Simulate real GC: create a lock, stuff a mock writer, drop all refs."""
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        lock = _make_async_lock()
        lock._writer = mock_writer
        lock._closed = False
        del lock
        gc.collect()

        mock_writer.close.assert_called_once()


# ===========================================================================
# Async aclose() bounds wait_closed with a timeout
# ===========================================================================


class TestAsyncAcloseWaitClosedTimeout:
    async def test_aclose_with_fast_wait_closed(self):
        """Normal case: wait_closed() returns quickly, aclose succeeds."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close = MagicMock()

        wait_closed_future = asyncio.get_event_loop().create_future()
        wait_closed_future.set_result(None)
        mock_writer.wait_closed = MagicMock(return_value=wait_closed_future)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        await lock.aclose()

        mock_writer.close.assert_called_once()
        mock_writer.wait_closed.assert_called_once()
        assert lock._writer is None
        assert lock._reader is None
        assert lock.token is None
        assert lock._closed is True

    async def test_aclose_times_out_wait_closed(self):
        """If wait_closed() hangs, aclose should still complete within ~5s."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close = MagicMock()

        # wait_closed that never resolves
        never_done = asyncio.get_event_loop().create_future()
        mock_writer.wait_closed = MagicMock(return_value=never_done)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        # aclose should not hang — the wait_for timeout should fire.
        # Use a tighter overall timeout to keep the test fast.
        with patch("dflockd_client.client.asyncio.wait_for", new_callable=lambda: AsyncMock) as mock_wait_for:
            mock_wait_for.side_effect = asyncio.TimeoutError()
            await lock.aclose()

        # Despite timeout, cleanup should be complete
        assert lock._writer is None
        assert lock._reader is None
        assert lock._closed is True

    async def test_aclose_wait_closed_exception_suppressed(self):
        """If wait_closed() raises an arbitrary exception, aclose suppresses it."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close = MagicMock()

        error_future = asyncio.get_event_loop().create_future()
        error_future.set_exception(OSError("broken pipe"))
        mock_writer.wait_closed = MagicMock(return_value=error_future)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        await lock.aclose()  # should not raise

        assert lock._writer is None
        assert lock._closed is True

    async def test_aclose_idempotent(self):
        """Calling aclose() twice is a no-op the second time."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close = MagicMock()

        wait_closed_future = asyncio.get_event_loop().create_future()
        wait_closed_future.set_result(None)
        mock_writer.wait_closed = MagicMock(return_value=wait_closed_future)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        await lock.aclose()
        await lock.aclose()  # second call is a no-op

        mock_writer.close.assert_called_once()

    async def test_aclose_real_timeout_bound(self):
        """Integration-style: verify aclose returns promptly even with a hanging wait_closed."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close = MagicMock()

        # Create a future that will never complete on its own
        hang_forever = asyncio.get_event_loop().create_future()
        mock_writer.wait_closed = MagicMock(return_value=hang_forever)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        # aclose has a 5s timeout internally; we verify it doesn't hang
        # by applying our own tighter deadline
        try:
            await asyncio.wait_for(lock.aclose(), timeout=10)
        except asyncio.TimeoutError:
            pytest.fail("aclose() hung beyond 10s — wait_closed timeout is broken")

        assert lock._writer is None
        assert lock._closed is True


# ===========================================================================
# Sync renew loop drops local refs on exit
# ===========================================================================


class TestSyncRenewLoopLocalRefCleanup:
    def test_renew_loop_releases_refs_on_stop_event(self):
        """When _stop_event is set, the renew loop exits and drops local refs."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.gettimeout.return_value = None
        mock_rfile = MagicMock(spec=io.TextIOWrapper)

        lock._sock = mock_sock
        lock._rfile = mock_rfile
        lock.token = "fake-token"
        lock.lease = 10

        # Pre-set the stop event so the loop exits immediately
        lock._stop_event.set()

        lock._renew_loop()

        # After the loop exits, the method should have returned.
        # We can't directly inspect locals, but we verify the loop
        # didn't error out and the lock state is intact.
        assert lock._sock is mock_sock  # not cleared by the loop itself

    def test_renew_loop_releases_refs_on_renew_failure(self):
        """When proto_renew raises, the loop logs and exits, dropping local refs."""
        lock = _make_sync_lock(lease_ttl_s=2, renew_ratio=0.1)
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.gettimeout.return_value = None
        mock_rfile = MagicMock(spec=io.TextIOWrapper)

        lock._sock = mock_sock
        lock._rfile = mock_rfile
        lock.token = "fake-token"
        lock.lease = 2

        call_count = 0

        def _failing_renew(sock, rfile, token):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("connection lost")

        lock._proto_renew = _failing_renew

        # Let the loop run — it should fail on first renew and exit
        lock._renew_loop()

        assert call_count == 1

    def test_renew_loop_noop_when_no_socket(self):
        """If _sock is None at start, renew loop returns immediately."""
        lock = _make_sync_lock()
        assert lock._sock is None
        lock._renew_loop()  # should not raise or hang

    def test_renew_loop_noop_when_no_token(self):
        """If token is None at start, renew loop returns immediately."""
        lock = _make_sync_lock()
        lock._sock = MagicMock(spec=socket.socket)
        lock._rfile = MagicMock(spec=io.TextIOWrapper)
        lock.token = None
        lock._renew_loop()  # should not raise or hang


# ===========================================================================
# Async renew loop drops local refs on exit
# ===========================================================================


class TestAsyncRenewLoopLocalRefCleanup:
    async def test_renew_loop_exits_when_closed(self):
        """When _closed is set, the renew loop should exit."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_reader = MagicMock(spec=asyncio.StreamReader)

        lock._writer = mock_writer
        lock._reader = mock_reader
        lock.token = "fake-token"
        lock.lease = 10
        lock._closed = True  # pre-close so loop exits after first sleep

        # The loop should exit quickly since _closed is True
        try:
            await asyncio.wait_for(lock._renew_loop(), timeout=3)
        except asyncio.TimeoutError:
            pytest.fail("_renew_loop hung despite _closed=True")

    async def test_renew_loop_exits_on_cancellation(self):
        """Cancelling the renew task causes it to exit (it catches CancelledError)."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_reader = MagicMock(spec=asyncio.StreamReader)

        lock._writer = mock_writer
        lock._reader = mock_reader
        lock.token = "fake-token"
        lock.lease = 100  # long lease so it sleeps

        task = asyncio.create_task(lock._renew_loop())
        await asyncio.sleep(0.05)
        task.cancel()

        # _renew_loop catches CancelledError and returns normally
        try:
            await asyncio.wait_for(task, timeout=3)
        except asyncio.TimeoutError:
            pytest.fail("_renew_loop did not exit after cancellation")

    async def test_renew_loop_noop_when_no_writer(self):
        """If _writer is None at start, renew loop returns immediately."""
        lock = _make_async_lock()
        assert lock._writer is None
        await lock._renew_loop()  # should not raise or hang

    async def test_renew_loop_noop_when_no_token(self):
        """If token is None at start, renew loop returns immediately."""
        lock = _make_async_lock()
        lock._writer = MagicMock(spec=asyncio.StreamWriter)
        lock._reader = MagicMock(spec=asyncio.StreamReader)
        lock.token = None
        await lock._renew_loop()  # should not raise or hang

    async def test_renew_loop_exits_on_renew_failure(self):
        """When proto_renew raises, the loop logs and exits."""
        lock = _make_async_lock(lease_ttl_s=2, renew_ratio=0.01)
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_reader = MagicMock(spec=asyncio.StreamReader)

        lock._writer = mock_writer
        lock._reader = mock_reader
        lock.token = "fake-token"
        lock.lease = 2

        call_count = 0

        async def _failing_renew(reader, writer, token):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("connection lost")

        lock._proto_renew = _failing_renew

        try:
            await asyncio.wait_for(lock._renew_loop(), timeout=5)
        except asyncio.TimeoutError:
            pytest.fail("_renew_loop hung after renew failure")

        assert call_count == 1


# ===========================================================================
# Sync close() is robust
# ===========================================================================


class TestSyncCloseRobustness:
    def test_close_idempotent(self):
        """Calling close() multiple times is safe."""
        lock = _make_sync_lock()
        lock.close()
        lock.close()
        lock.close()
        assert lock._closed is True

    def test_close_cleans_up_all_state(self):
        """close() nils out _sock, _rfile, and token."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_rfile = MagicMock(spec=io.TextIOWrapper)
        lock._sock = mock_sock
        lock._rfile = mock_rfile
        lock.token = "fake-token"
        lock._closed = False

        lock.close()

        assert lock._sock is None
        assert lock._rfile is None
        assert lock.token is None
        assert lock._closed is True
        mock_rfile.close.assert_called_once()
        mock_sock.close.assert_called_once()

    def test_close_tolerates_rfile_close_error(self):
        """If rfile.close() raises, sock still gets closed."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_rfile = MagicMock(spec=io.TextIOWrapper)
        mock_rfile.close.side_effect = OSError("broken")
        lock._sock = mock_sock
        lock._rfile = mock_rfile
        lock._closed = False

        lock.close()

        mock_sock.close.assert_called_once()
        assert lock._sock is None

    def test_close_tolerates_sock_close_error(self):
        """If sock.close() raises, state is still cleaned up."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.close.side_effect = OSError("broken")
        lock._sock = mock_sock
        lock._rfile = MagicMock(spec=io.TextIOWrapper)
        lock._closed = False

        lock.close()

        assert lock._sock is None
        assert lock._rfile is None

    def test_close_shutdown_before_close(self):
        """close() calls shutdown(SHUT_RDWR) before acquiring _io_lock."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        lock._sock = mock_sock
        lock._rfile = MagicMock(spec=io.TextIOWrapper)
        lock._closed = False

        lock.close()

        mock_sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)

    def test_close_shutdown_tolerates_error(self):
        """shutdown() failure (e.g. not connected) doesn't prevent close()."""
        lock = _make_sync_lock()
        mock_sock = MagicMock(spec=socket.socket)
        mock_sock.shutdown.side_effect = OSError("not connected")
        lock._sock = mock_sock
        lock._rfile = MagicMock(spec=io.TextIOWrapper)
        lock._closed = False

        lock.close()

        mock_sock.close.assert_called_once()
        assert lock._sock is None


# ===========================================================================
# Async aclose() is robust
# ===========================================================================


class TestAsyncAcloseRobustness:
    async def test_aclose_cleans_up_all_state(self):
        """aclose() nils out _writer, _reader, and token."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)

        wait_closed_future = asyncio.get_event_loop().create_future()
        wait_closed_future.set_result(None)
        mock_writer.wait_closed = MagicMock(return_value=wait_closed_future)

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock.token = "fake-token"
        lock._closed = False

        await lock.aclose()

        assert lock._writer is None
        assert lock._reader is None
        assert lock.token is None
        assert lock._closed is True

    async def test_aclose_tolerates_close_error(self):
        """If writer.close() raises, state is still cleaned up."""
        lock = _make_async_lock()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        mock_writer.close.side_effect = RuntimeError("transport closed")

        lock._writer = mock_writer
        lock._reader = MagicMock()
        lock._closed = False

        await lock.aclose()

        assert lock._writer is None
        assert lock._closed is True


# ===========================================================================
# _common.py: encode_lines, parse_lease
# ===========================================================================


class TestEncodeLines:
    def test_basic(self):
        from dflockd_client._common import encode_lines
        assert encode_lines("a", "b") == b"a\nb\n"

    def test_rejects_newlines(self):
        from dflockd_client._common import encode_lines
        with pytest.raises(ValueError, match="newlines"):
            encode_lines("bad\nline")

    def test_rejects_cr(self):
        from dflockd_client._common import encode_lines
        with pytest.raises(ValueError, match="newlines"):
            encode_lines("bad\rline")


class TestParseLease:
    def test_valid(self):
        from dflockd_client._common import parse_lease
        assert parse_lease(["ok", "token", "30"]) == 30

    def test_missing_lease_defaults_30(self):
        from dflockd_client._common import parse_lease
        assert parse_lease(["ok", "token"]) == 30

    def test_non_integer_defaults_30(self):
        from dflockd_client._common import parse_lease
        assert parse_lease(["ok", "token", "abc"]) == 30


# ===========================================================================
# Validation: renew_ratio, empty servers, semaphore limit
# ===========================================================================


class TestValidation:
    def test_renew_ratio_zero_raises(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            sclient.DistributedLock(key="k", renew_ratio=0)

    def test_renew_ratio_one_raises(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            sclient.DistributedLock(key="k", renew_ratio=1.0)

    def test_renew_ratio_negative_raises(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            sclient.DistributedLock(key="k", renew_ratio=-0.5)

    def test_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            sclient.DistributedLock(key="k", servers=[])

    def test_semaphore_limit_zero_raises(self):
        with pytest.raises(ValueError, match="limit"):
            sclient.DistributedSemaphore(key="k", limit=0)

    def test_semaphore_limit_negative_raises(self):
        with pytest.raises(ValueError, match="limit"):
            sclient.DistributedSemaphore(key="k", limit=-1)

    def test_async_renew_ratio_zero_raises(self):
        with pytest.raises(ValueError, match="renew_ratio"):
            aclient.DistributedLock(key="k", renew_ratio=0)

    def test_async_empty_servers_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            aclient.DistributedLock(key="k", servers=[])

    def test_async_semaphore_limit_zero_raises(self):
        with pytest.raises(ValueError, match="limit"):
            aclient.DistributedSemaphore(key="k", limit=0)


# ===========================================================================
# Async readline guards
# ===========================================================================


class TestAsyncReadline:
    async def test_eof_raises_connection_error(self):
        reader = asyncio.StreamReader()
        reader.feed_eof()
        with pytest.raises(ConnectionError, match="server closed"):
            await aclient._readline(reader)

    async def test_normal_line(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"ok token123 30\n")
        reader.feed_eof()
        result = await aclient._readline(reader)
        assert result == "ok token123 30"

    async def test_strips_crlf(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"ok token123 30\r\n")
        reader.feed_eof()
        result = await aclient._readline(reader)
        assert result == "ok token123 30"


class TestSyncReadline:
    def test_eof_raises_connection_error(self):
        buf = io.StringIO("")
        with pytest.raises(ConnectionError, match="server closed"):
            sclient._readline(buf)

    def test_normal_line(self):
        buf = io.StringIO("ok token123 30\n")
        result = sclient._readline(buf)
        assert result == "ok token123 30"

    def test_strips_crlf(self):
        buf = io.StringIO("ok token123 30\r\n")
        result = sclient._readline(buf)
        assert result == "ok token123 30"


# ===========================================================================
# Sync _stop_renew behaviour
# ===========================================================================


class TestSyncStopRenew:
    def test_stop_renew_noop_when_no_thread(self):
        """_stop_renew with no thread should be a no-op."""
        lock = _make_sync_lock()
        assert lock._renew_thread is None
        lock._stop_renew()  # should not raise

    def test_stop_renew_joins_thread(self):
        """_stop_renew sets the stop event and joins the thread."""
        lock = _make_sync_lock()

        exited = threading.Event()

        def _fake_loop():
            lock._stop_event.wait()
            exited.set()

        lock._renew_thread = threading.Thread(target=_fake_loop, daemon=True)
        lock._renew_thread.start()

        lock._stop_renew()

        assert exited.is_set()
        assert lock._renew_thread is None


# ===========================================================================
# Async _cancel_renew behaviour
# ===========================================================================


class TestAsyncCancelRenew:
    async def test_cancel_renew_noop_when_no_task(self):
        """_cancel_renew with no task should be a no-op."""
        lock = _make_async_lock()
        assert lock._renew_task is None
        await lock._cancel_renew()  # should not raise

    async def test_cancel_renew_cancels_task(self):
        """_cancel_renew cancels a running task and waits for it."""
        lock = _make_async_lock()

        async def _hang():
            await asyncio.sleep(3600)

        lock._renew_task = asyncio.create_task(_hang())
        await asyncio.sleep(0.01)

        await lock._cancel_renew()

        assert lock._renew_task is None


# ===========================================================================
# DEFAULT_SERVERS immutability
# ===========================================================================


class TestDefaultServers:
    def test_default_servers_is_tuple(self):
        """DEFAULT_SERVERS should be a tuple (immutable) to prevent accidental mutation."""
        from dflockd_client.sharding import DEFAULT_SERVERS
        assert isinstance(DEFAULT_SERVERS, tuple)

    def test_lock_servers_is_copy(self):
        """Each lock's servers list should be independent of DEFAULT_SERVERS."""
        lock1 = sclient.DistributedLock(key="k1")
        lock2 = sclient.DistributedLock(key="k2")
        lock1.servers.append(("extra", 1234))
        assert ("extra", 1234) not in lock2.servers
