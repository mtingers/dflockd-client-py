"""Unit tests for resource cleanup, protocol error paths, and bug fix regression.

These tests use mocking/fakes — no dflockd server required.
"""

import asyncio
import gc
import inspect
import io
import json
import socket
import threading
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dflockd_client.client as aclient
import dflockd_client.sync_client as sclient
from dflockd_client._common import (
    _MAX_LINE_LEN,
    DflockdTimeoutError,
    DrainingError,
    MaxLocksError,
    NotQueuedError,
)


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
# SignalConn __del__ — closes underlying socket / writer if user forgot to close
# ===========================================================================


class TestSyncSignalConnDel:
    def test_del_closes_socket_on_gc(self):
        sc = sclient.SignalConn()
        mock_sock = MagicMock(spec=socket.socket)
        sc._sock = mock_sock

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sc.__del__()
            sc._sock = None

        mock_sock.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1
        assert "garbage collected" in str(resource_warnings[0].message)

    def test_del_no_warning_when_never_connected(self):
        sc = sclient.SignalConn()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sc.__del__()
        assert len(w) == 0


class TestAsyncSignalConnDel:
    def test_del_closes_writer_on_gc(self):
        sc = aclient.SignalConn()
        mock_writer = MagicMock(spec=asyncio.StreamWriter)
        sc._writer = mock_writer

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sc.__del__()
            sc._writer = None

        mock_writer.close.assert_called_once()
        resource_warnings = [x for x in w if issubclass(x.category, ResourceWarning)]
        assert len(resource_warnings) == 1
        assert "garbage collected" in str(resource_warnings[0].message)

    def test_del_no_warning_when_never_connected(self):
        sc = aclient.SignalConn()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sc.__del__()
        assert len(w) == 0


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
        with patch(
            "dflockd_client.client.asyncio.wait_for", new_callable=lambda: AsyncMock
        ) as mock_wait_for:
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

    def test_missing_lease_raises(self):
        from dflockd_client._common import parse_lease

        with pytest.raises(RuntimeError, match="bad ok response"):
            parse_lease(["ok", "token"])

    def test_non_integer_raises(self):
        from dflockd_client._common import parse_lease

        with pytest.raises(RuntimeError, match="bad ok response"):
            parse_lease(["ok", "token", "abc"])


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


# ===========================================================================
# Client-side protocol validation
# ===========================================================================


class TestProtocolValidation:
    async def test_async_rejects_empty_key_before_io(self):
        with pytest.raises(ValueError, match="key must not be empty"):
            await aclient.acquire(None, None, "", 1)  # type: ignore[arg-type]

    async def test_async_rejects_whitespace_key_before_io(self):
        with pytest.raises(ValueError, match="key must not contain whitespace"):
            await aclient.acquire(None, None, "bad key", 1)  # type: ignore[arg-type]

    async def test_async_rejects_overlong_key_before_io(self):
        with pytest.raises(ValueError, match="key too long"):
            await aclient.acquire(None, None, "x" * 257, 1)  # type: ignore[arg-type]

    async def test_async_rejects_negative_timeout_before_io(self):
        with pytest.raises(ValueError, match="acquire_timeout_s"):
            await aclient.acquire(None, None, "k", -1)  # type: ignore[arg-type]

    async def test_async_rejects_float_timeout_before_io(self):
        with pytest.raises(TypeError, match="acquire_timeout_s"):
            await aclient.acquire(None, None, "k", 1.5)  # type: ignore[arg-type]

    async def test_async_rejects_zero_lease_before_io(self):
        with pytest.raises(ValueError, match="lease_ttl_s"):
            await aclient.acquire(None, None, "k", 1, lease_ttl_s=0)  # type: ignore[arg-type]

    async def test_async_rejects_bool_limit_before_io(self):
        with pytest.raises(TypeError, match="limit"):
            await aclient.sem_acquire(None, None, "k", 1, True)  # type: ignore[arg-type]

    async def test_async_rejects_token_with_whitespace_before_io(self):
        with pytest.raises(ValueError, match="token"):
            await aclient.release(None, None, "k", "bad token")  # type: ignore[arg-type]

    def test_sync_rejects_empty_key_before_io(self):
        with pytest.raises(ValueError, match="key must not be empty"):
            sclient.acquire(None, None, "", 1)  # type: ignore[arg-type]

    def test_sync_rejects_negative_timeout_before_io(self):
        with pytest.raises(ValueError, match="acquire_timeout_s"):
            sclient.acquire(None, None, "k", -1)  # type: ignore[arg-type]

    def test_sync_rejects_float_timeout_before_io(self):
        with pytest.raises(TypeError, match="acquire_timeout_s"):
            sclient.acquire(None, None, "k", 1.5)  # type: ignore[arg-type]

    def test_sync_rejects_zero_lease_before_io(self):
        with pytest.raises(ValueError, match="lease_ttl_s"):
            sclient.acquire(None, None, "k", 1, lease_ttl_s=0)  # type: ignore[arg-type]

    def test_sync_rejects_bool_limit_before_io(self):
        with pytest.raises(TypeError, match="limit"):
            sclient.sem_acquire(None, None, "k", 1, True)  # type: ignore[arg-type]

    def test_sync_rejects_token_with_whitespace_before_io(self):
        with pytest.raises(ValueError, match="token"):
            sclient.release(None, None, "k", "bad token")  # type: ignore[arg-type]

    def test_distributed_semaphore_limit_is_keyword_only(self):
        async_params = inspect.signature(aclient.DistributedSemaphore).parameters
        sync_params = inspect.signature(sclient.DistributedSemaphore).parameters
        assert async_params["limit"].kind is inspect.Parameter.KEYWORD_ONLY
        assert sync_params["limit"].kind is inspect.Parameter.KEYWORD_ONLY

        with pytest.raises(TypeError):
            aclient.DistributedSemaphore("k", 2)
        with pytest.raises(TypeError):
            sclient.DistributedSemaphore("k", 2)

    async def test_async_empty_auth_token_is_not_sent(self):
        reader = asyncio.StreamReader()
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        async def fake_open_connection(*_args, **_kwargs):
            return reader, writer

        lock = _make_async_lock(auth_token="")
        with patch(
            "dflockd_client.client.asyncio.open_connection",
            side_effect=fake_open_connection,
        ):
            try:
                await lock._connect()
                writer.write.assert_not_called()
            finally:
                await lock.aclose()

    async def test_async_auth_draining_is_not_permission_error(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"error_draining\n")
        writer = MagicMock(spec=asyncio.StreamWriter)
        writer.write = MagicMock()
        writer.drain = AsyncMock()
        writer.close = MagicMock()
        writer.wait_closed = AsyncMock()

        async def fake_open_connection(*_args, **_kwargs):
            return reader, writer

        lock = _make_async_lock(auth_token="secret")
        with patch(
            "dflockd_client.client.asyncio.open_connection",
            side_effect=fake_open_connection,
        ):
            with pytest.raises(DrainingError, match="authentication failed"):
                await lock._connect()

    def test_sync_empty_auth_token_is_not_sent(self):
        sock = MagicMock(spec=socket.socket)
        sock.makefile.return_value = io.StringIO("")
        lock = _make_sync_lock(auth_token="")

        with patch(
            "dflockd_client.sync_client.socket.create_connection",
            return_value=sock,
        ):
            try:
                lock._connect()
                sock.sendall.assert_not_called()
            finally:
                lock.close()

    def test_sync_auth_draining_is_not_permission_error(self):
        sock = MagicMock(spec=socket.socket)
        sock.makefile.return_value = io.StringIO("error_draining\n")
        lock = _make_sync_lock(auth_token="secret")

        with patch(
            "dflockd_client.sync_client.socket.create_connection",
            return_value=sock,
        ):
            with pytest.raises(DrainingError, match="authentication failed"):
                lock._connect()


# ===========================================================================
# High-level timeout handling
# ===========================================================================


class TestHighLevelTimeoutHandling:
    async def test_async_protocol_timeout_returns_false(self):
        lock = _make_async_lock(acquire_timeout_s=1)

        async def fake_connect():
            return object(), object()

        async def fake_acquire(_reader, _writer):
            raise DflockdTimeoutError("timeout acquiring 'k'")

        lock._connect = fake_connect  # type: ignore[method-assign]
        lock._proto_acquire = fake_acquire  # type: ignore[method-assign]

        assert await lock.acquire() is False

    async def test_async_io_timeout_raises(self, monkeypatch):
        lock = _make_async_lock(acquire_timeout_s=0)

        async def fake_connect():
            return object(), object()

        async def slow_acquire(_reader, _writer):
            await asyncio.sleep(1)

        monkeypatch.setattr(aclient, "_IO_TIMEOUT_SLACK_S", 0.01)
        lock._connect = fake_connect  # type: ignore[method-assign]
        lock._proto_acquire = slow_acquire  # type: ignore[method-assign]

        with pytest.raises(TimeoutError):
            await lock.acquire()

    def test_sync_protocol_timeout_returns_false(self):
        lock = _make_sync_lock(acquire_timeout_s=1)
        sock = MagicMock(spec=socket.socket)

        def fake_connect():
            return sock, object()

        def fake_acquire(_sock, _rfile):
            raise DflockdTimeoutError("timeout acquiring 'k'")

        lock._connect = fake_connect  # type: ignore[method-assign]
        lock._proto_acquire = fake_acquire  # type: ignore[method-assign]

        assert lock.acquire() is False

    def test_sync_io_timeout_raises(self):
        lock = _make_sync_lock(acquire_timeout_s=1)
        sock = MagicMock(spec=socket.socket)

        def fake_connect():
            return sock, object()

        def fake_acquire(_sock, _rfile):
            raise TimeoutError("socket timed out")

        lock._connect = fake_connect  # type: ignore[method-assign]
        lock._proto_acquire = fake_acquire  # type: ignore[method-assign]

        with pytest.raises(TimeoutError, match="socket timed out"):
            lock.acquire()


# ===========================================================================
# Async protocol function error paths (mocked reader/writer, no server)
# ===========================================================================


def _async_rw(response: bytes):
    """Create a mock async reader (pre-fed) and writer."""
    reader = asyncio.StreamReader()
    reader.feed_data(response)
    writer = MagicMock(spec=asyncio.StreamWriter)
    writer.write = MagicMock()
    writer.drain = AsyncMock()
    return reader, writer


class TestAsyncProtocolErrors:
    async def test_readline_value_error_becomes_runtime_error(self):
        reader = asyncio.StreamReader(limit=10)
        reader.feed_data(b"x" * 30 + b"\n")
        with pytest.raises(RuntimeError, match="line length limit"):
            await aclient._readline(reader)

    # --- acquire ---

    async def test_acquire_ok(self):
        r, w = _async_rw(b"ok tok123 30\n")
        token, lease = await aclient.acquire(r, w, "k", 5)
        assert token == "tok123"
        assert lease == 30

    async def test_acquire_timeout_response(self):
        r, w = _async_rw(b"timeout\n")
        with pytest.raises(TimeoutError, match="timeout acquiring"):
            await aclient.acquire(r, w, "k", 5)

    async def test_acquire_bad_response(self):
        r, w = _async_rw(b"error something\n")
        with pytest.raises(RuntimeError, match="acquire failed"):
            await aclient.acquire(r, w, "k", 5)

    async def test_acquire_max_locks_status_is_typed(self):
        r, w = _async_rw(b"error_max_locks\n")
        with pytest.raises(MaxLocksError, match="acquire failed"):
            await aclient.acquire(r, w, "k", 5)

    async def test_acquire_short_ok(self):
        r, w = _async_rw(b"ok \n")
        with pytest.raises(RuntimeError, match="bad ok response"):
            await aclient.acquire(r, w, "k", 5)

    # --- renew ---

    async def test_renew_ok_with_remaining(self):
        r, w = _async_rw(b"ok 42\n")
        result = await aclient.renew(r, w, "k", "tok")
        assert result == 42

    async def test_renew_bare_ok_raises(self):
        r, w = _async_rw(b"ok\n")
        with pytest.raises(RuntimeError, match="bad renew response"):
            await aclient.renew(r, w, "k", "tok")

    async def test_renew_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="renew failed"):
            await aclient.renew(r, w, "k", "tok")

    # --- enqueue ---

    async def test_enqueue_acquired(self):
        r, w = _async_rw(b"acquired tok123 30\n")
        status, token, lease = await aclient.enqueue(r, w, "k")
        assert (status, token, lease) == ("acquired", "tok123", 30)

    async def test_enqueue_queued(self):
        r, w = _async_rw(b"queued\n")
        status, token, lease = await aclient.enqueue(r, w, "k")
        assert (status, token, lease) == ("queued", None, None)

    async def test_enqueue_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="enqueue failed"):
            await aclient.enqueue(r, w, "k")

    # --- wait ---

    async def test_wait_ok(self):
        r, w = _async_rw(b"ok tok456 60\n")
        token, lease = await aclient.wait(r, w, "k", 5)
        assert (token, lease) == ("tok456", 60)

    async def test_wait_timeout_response(self):
        r, w = _async_rw(b"timeout\n")
        with pytest.raises(TimeoutError, match="timeout waiting"):
            await aclient.wait(r, w, "k", 5)

    async def test_wait_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="wait failed"):
            await aclient.wait(r, w, "k", 5)

    async def test_wait_not_queued_status_is_typed(self):
        r, w = _async_rw(b"error_not_enqueued\n")
        with pytest.raises(NotQueuedError, match="wait failed"):
            await aclient.wait(r, w, "k", 5)

    # --- release ---

    async def test_release_ok(self):
        r, w = _async_rw(b"ok\n")
        await aclient.release(r, w, "k", "tok")  # should not raise

    async def test_release_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="release failed"):
            await aclient.release(r, w, "k", "tok")

    # --- stats ---

    async def test_stats_ok(self):
        payload = json.dumps(
            {
                "connections": 1,
                "locks": [],
                "semaphores": [],
                "idle_locks": [],
                "idle_semaphores": [],
            }
        )
        r, w = _async_rw(f"ok {payload}\n".encode())
        result = await aclient.stats(r, w)
        assert result["connections"] == 1

    async def test_stats_bad_response(self):
        r, w = _async_rw(b"error\n")
        with pytest.raises(RuntimeError, match="stats failed"):
            await aclient.stats(r, w)

    async def test_stats_bad_json(self):
        r, w = _async_rw(b"ok {not valid json\n")
        with pytest.raises(RuntimeError, match="bad stats response"):
            await aclient.stats(r, w)

    # --- semaphore wrappers ---

    async def test_sem_acquire_bad_limit(self):
        with pytest.raises(ValueError, match="limit"):
            await aclient.sem_acquire(None, None, "k", 5, 0)  # type: ignore[arg-type]

    async def test_sem_enqueue_bad_limit(self):
        with pytest.raises(ValueError, match="limit"):
            await aclient.sem_enqueue(None, None, "k", 0)  # type: ignore[arg-type]

    async def test_sem_acquire_timeout_labels_semaphore(self):
        r, w = _async_rw(b"timeout\n")
        with pytest.raises(TimeoutError, match="semaphore"):
            await aclient.sem_acquire(r, w, "k", 5, 2)

    async def test_sem_renew_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="sem_renew failed"):
            await aclient.sem_renew(r, w, "k", "tok")

    async def test_sem_release_bad_response(self):
        r, w = _async_rw(b"error bad\n")
        with pytest.raises(RuntimeError, match="sem_release failed"):
            await aclient.sem_release(r, w, "k", "tok")

    # --- sig_emit ---

    async def test_sig_emit_ok(self):
        r, w = _async_rw(b"ok 3\n")
        n = await aclient.sig_emit(r, w, "ch.test", "hello")
        assert n == 3

    async def test_sig_emit_bad_response(self):
        r, w = _async_rw(b"error\n")
        with pytest.raises(RuntimeError, match="signal failed"):
            await aclient.sig_emit(r, w, "ch.test", "hello")

    async def test_sig_emit_bad_count(self):
        r, w = _async_rw(b"ok notanumber\n")
        with pytest.raises(RuntimeError, match="bad signal response"):
            await aclient.sig_emit(r, w, "ch.test", "hello")


# ===========================================================================
# Sync protocol function error paths (mocked socket/rfile, no server)
# ===========================================================================


def _sync_rw(response: str):
    """Create a mock sync socket and StringIO rfile."""
    sock = MagicMock(spec=socket.socket)
    rfile = io.StringIO(response)
    return sock, rfile


class TestSyncProtocolErrors:
    def test_readline_too_long(self):
        huge = "x" * (_MAX_LINE_LEN + 10) + "\n"
        buf = io.StringIO(huge)
        with pytest.raises(RuntimeError, match="too large"):
            sclient._readline(buf)  # type: ignore[arg-type]

    # --- acquire ---

    def test_acquire_ok(self):
        s, r = _sync_rw("ok tok123 30\n")
        token, lease = sclient.acquire(s, r, "k", 5)  # type: ignore[arg-type]
        assert (token, lease) == ("tok123", 30)

    def test_acquire_timeout_response(self):
        s, r = _sync_rw("timeout\n")
        with pytest.raises(TimeoutError, match="timeout acquiring"):
            sclient.acquire(s, r, "k", 5)  # type: ignore[arg-type]

    def test_acquire_bad_response(self):
        s, r = _sync_rw("error something\n")
        with pytest.raises(RuntimeError, match="acquire failed"):
            sclient.acquire(s, r, "k", 5)  # type: ignore[arg-type]

    def test_acquire_max_locks_status_is_typed(self):
        s, r = _sync_rw("error_max_locks\n")
        with pytest.raises(MaxLocksError, match="acquire failed"):
            sclient.acquire(s, r, "k", 5)  # type: ignore[arg-type]

    def test_acquire_short_ok(self):
        s, r = _sync_rw("ok \n")
        with pytest.raises(RuntimeError, match="bad ok response"):
            sclient.acquire(s, r, "k", 5)  # type: ignore[arg-type]

    # --- renew ---

    def test_renew_ok_with_remaining(self):
        s, r = _sync_rw("ok 42\n")
        result = sclient.renew(s, r, "k", "tok")  # type: ignore[arg-type]
        assert result == 42

    def test_renew_bare_ok_raises(self):
        s, r = _sync_rw("ok\n")
        with pytest.raises(RuntimeError, match="bad renew response"):
            sclient.renew(s, r, "k", "tok")  # type: ignore[arg-type]

    def test_renew_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="renew failed"):
            sclient.renew(s, r, "k", "tok")  # type: ignore[arg-type]

    # --- enqueue ---

    def test_enqueue_acquired(self):
        s, r = _sync_rw("acquired tok123 30\n")
        status, token, lease = sclient.enqueue(s, r, "k")  # type: ignore[arg-type]
        assert (status, token, lease) == ("acquired", "tok123", 30)

    def test_enqueue_queued(self):
        s, r = _sync_rw("queued\n")
        status, token, lease = sclient.enqueue(s, r, "k")  # type: ignore[arg-type]
        assert (status, token, lease) == ("queued", None, None)

    def test_enqueue_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="enqueue failed"):
            sclient.enqueue(s, r, "k")  # type: ignore[arg-type]

    # --- wait ---

    def test_wait_ok(self):
        s, r = _sync_rw("ok tok456 60\n")
        token, lease = sclient.wait(s, r, "k", 5)  # type: ignore[arg-type]
        assert (token, lease) == ("tok456", 60)

    def test_wait_timeout_response(self):
        s, r = _sync_rw("timeout\n")
        with pytest.raises(TimeoutError, match="timeout waiting"):
            sclient.wait(s, r, "k", 5)  # type: ignore[arg-type]

    def test_wait_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="wait failed"):
            sclient.wait(s, r, "k", 5)  # type: ignore[arg-type]

    def test_wait_not_queued_status_is_typed(self):
        s, r = _sync_rw("error_not_enqueued\n")
        with pytest.raises(NotQueuedError, match="wait failed"):
            sclient.wait(s, r, "k", 5)  # type: ignore[arg-type]

    # --- release ---

    def test_release_ok(self):
        s, r = _sync_rw("ok\n")
        sclient.release(s, r, "k", "tok")  # type: ignore[arg-type]

    def test_release_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="release failed"):
            sclient.release(s, r, "k", "tok")  # type: ignore[arg-type]

    # --- stats ---

    def test_stats_ok(self):
        payload = json.dumps(
            {
                "connections": 1,
                "locks": [],
                "semaphores": [],
                "idle_locks": [],
                "idle_semaphores": [],
            }
        )
        s, r = _sync_rw(f"ok {payload}\n")
        result = sclient.stats(s, r)  # type: ignore[arg-type]
        assert result["connections"] == 1

    def test_stats_bad_response(self):
        s, r = _sync_rw("error\n")
        with pytest.raises(RuntimeError, match="stats failed"):
            sclient.stats(s, r)  # type: ignore[arg-type]

    def test_stats_bad_json(self):
        s, r = _sync_rw("ok {not valid json\n")
        with pytest.raises(RuntimeError, match="bad stats response"):
            sclient.stats(s, r)  # type: ignore[arg-type]

    # --- semaphore wrappers ---

    def test_sem_acquire_bad_limit(self):
        with pytest.raises(ValueError, match="limit"):
            sclient.sem_acquire(None, None, "k", 5, 0)  # type: ignore[arg-type]

    def test_sem_enqueue_bad_limit(self):
        with pytest.raises(ValueError, match="limit"):
            sclient.sem_enqueue(None, None, "k", 0)  # type: ignore[arg-type]

    def test_sem_acquire_timeout_labels_semaphore(self):
        s, r = _sync_rw("timeout\n")
        with pytest.raises(TimeoutError, match="semaphore"):
            sclient.sem_acquire(s, r, "k", 5, 2)  # type: ignore[arg-type]

    def test_sem_renew_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="sem_renew failed"):
            sclient.sem_renew(s, r, "k", "tok")  # type: ignore[arg-type]

    def test_sem_release_bad_response(self):
        s, r = _sync_rw("error bad\n")
        with pytest.raises(RuntimeError, match="sem_release failed"):
            sclient.sem_release(s, r, "k", "tok")  # type: ignore[arg-type]

    # --- sig_emit ---

    def test_sig_emit_ok(self):
        s, r = _sync_rw("ok 3\n")
        n = sclient.sig_emit(s, r, "ch.test", "hello")  # type: ignore[arg-type]
        assert n == 3

    def test_sig_emit_bad_response(self):
        s, r = _sync_rw("error\n")
        with pytest.raises(RuntimeError, match="signal failed"):
            sclient.sig_emit(s, r, "ch.test", "hello")  # type: ignore[arg-type]

    def test_sig_emit_bad_count(self):
        s, r = _sync_rw("ok notanumber\n")
        with pytest.raises(RuntimeError, match="bad signal response"):
            sclient.sig_emit(s, r, "ch.test", "hello")  # type: ignore[arg-type]
