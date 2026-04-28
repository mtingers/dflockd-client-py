"""Tests for signal (pub/sub) functionality — async and sync clients."""

import asyncio
import io
import socket
import threading

import pytest

import dflockd_client.client as aclient
import dflockd_client.sync_client as sclient
from dflockd_client._common import Signal


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
# Unit tests (no server required)
# ===========================================================================


class TestSignalType:
    def test_named_tuple(self):
        sig = Signal(channel="events.login", payload="alice")
        assert sig.channel == "events.login"
        assert sig.payload == "alice"
        assert sig == Signal("events.login", "alice")

    def test_unpack(self):
        ch, pl = Signal("a.b", "data")
        assert ch == "a.b"
        assert pl == "data"


class TestSigEmitValidation:
    async def test_async_wildcard_star_rejected(self):
        with pytest.raises(ValueError, match="wildcards"):
            await aclient.sig_emit(None, None, "events.*.login", "x")  # type: ignore[arg-type]

    async def test_async_wildcard_gt_rejected(self):
        with pytest.raises(ValueError, match="wildcards"):
            await aclient.sig_emit(None, None, "events.>", "x")  # type: ignore[arg-type]

    def test_sync_wildcard_star_rejected(self):
        with pytest.raises(ValueError, match="wildcards"):
            sclient.sig_emit(None, None, "events.*.login", "x")  # type: ignore[arg-type]

    def test_sync_wildcard_gt_rejected(self):
        with pytest.raises(ValueError, match="wildcards"):
            sclient.sig_emit(None, None, "events.>", "x")  # type: ignore[arg-type]


class TestAsyncSignalConnEmitValidation:
    async def test_emit_rejects_wildcards(self):
        sc = aclient.SignalConn()
        # Not connected, but validation runs before any I/O
        with pytest.raises(ValueError, match="wildcards"):
            await sc.emit("events.*", "x")

    async def test_not_connected_raises(self):
        sc = aclient.SignalConn()
        with pytest.raises(RuntimeError, match="not connected"):
            await sc.listen("events.>")


class TestSyncSignalConnEmitValidation:
    def test_emit_rejects_wildcards(self):
        sc = sclient.SignalConn()
        with pytest.raises(ValueError, match="wildcards"):
            sc.emit("events.*", "x")

    def test_not_connected_raises(self):
        sc = sclient.SignalConn()
        with pytest.raises(RuntimeError, match="not connected"):
            sc.listen("events.>")


class TestSignalExports:
    def test_signal_in_package(self):
        from dflockd_client import Signal as S

        assert S is Signal

    def test_async_signal_conn_in_package(self):
        from dflockd_client import AsyncSignalConn

        assert AsyncSignalConn is aclient.SignalConn

    def test_sync_signal_conn_in_package(self):
        from dflockd_client import SyncSignalConn

        assert SyncSignalConn is sclient.SignalConn


# ===========================================================================
# Bug fix: connect() must reset _closed and create fresh queues
# ===========================================================================


class TestAsyncSignalConnClosedReset:
    async def test_connect_resets_closed_flag(self):
        sc = aclient.SignalConn(server=("127.0.0.1", 1), connect_timeout_s=0.1)
        sc._closed = True
        with pytest.raises((ConnectionRefusedError, OSError, asyncio.TimeoutError)):
            await sc.connect()
        # _closed was reset before the connection attempt
        assert sc._closed is False

    async def test_connect_creates_fresh_queue(self):
        sc = aclient.SignalConn(server=("127.0.0.1", 1), connect_timeout_s=0.1)
        sc._sig_queue.put_nowait(Signal("stale", "x"))
        assert not sc._sig_queue.empty()
        with pytest.raises((ConnectionRefusedError, OSError, asyncio.TimeoutError)):
            await sc.connect()
        assert sc._sig_queue.empty()


class TestSyncSignalConnClosedReset:
    def test_connect_resets_closed_flag(self):
        sc = sclient.SignalConn(server=("127.0.0.1", 1), connect_timeout_s=0.1)
        sc._closed = True
        with pytest.raises((ConnectionRefusedError, OSError)):
            sc.connect()
        assert sc._closed is False

    def test_connect_creates_fresh_queues(self):
        sc = sclient.SignalConn(server=("127.0.0.1", 1), connect_timeout_s=0.1)
        sc._sig_queue.put_nowait(Signal("stale", "x"))
        sc._resp_queue.put_nowait("stale")
        with pytest.raises((ConnectionRefusedError, OSError)):
            sc.connect()
        assert sc._sig_queue.empty()
        assert sc._resp_queue.empty()


# ===========================================================================
# Bug fix: None sentinel must be delivered even when queue is full
# ===========================================================================


class TestAsyncSentinelDeliveryWhenFull:
    async def test_sentinel_delivered_when_queue_full(self):
        sc = aclient.SignalConn()
        for i in range(64):
            sc._sig_queue.put_nowait(Signal(f"ch.{i}", str(i)))
        assert sc._sig_queue.full()

        # Simulate read_loop with an EOF reader
        reader = asyncio.StreamReader()
        reader.feed_eof()
        sc._reader = reader

        await sc._read_loop()

        items = []
        while not sc._sig_queue.empty():
            items.append(sc._sig_queue.get_nowait())
        assert items[-1] is None

    async def test_sentinel_works_with_empty_queue(self):
        sc = aclient.SignalConn()
        reader = asyncio.StreamReader()
        reader.feed_eof()
        sc._reader = reader

        await sc._read_loop()

        item = sc._sig_queue.get_nowait()
        assert item is None


class TestSyncSentinelDeliveryWhenFull:
    def test_sentinel_delivered_when_queue_full(self):
        sc = sclient.SignalConn()
        for i in range(64):
            sc._sig_queue.put_nowait(Signal(f"ch.{i}", str(i)))
        assert sc._sig_queue.full()

        sc._rfile = io.StringIO("")
        sc._read_loop()

        items = []
        while not sc._sig_queue.empty():
            items.append(sc._sig_queue.get_nowait())
        assert items[-1] is None

    def test_sentinel_works_with_empty_queue(self):
        sc = sclient.SignalConn()
        sc._rfile = io.StringIO("")

        sc._read_loop()

        item = sc._sig_queue.get_nowait()
        assert item is None


# ===========================================================================
# Heartbeat (ping) tests
# ===========================================================================


class TestAsyncHeartbeatDefault:
    def test_default_interval(self):
        sc = aclient.SignalConn()
        assert sc.heartbeat_interval_s == 15.0

    def test_custom_interval(self):
        sc = aclient.SignalConn(heartbeat_interval_s=5.0)
        assert sc.heartbeat_interval_s == 5.0

    def test_disabled_when_zero(self):
        sc = aclient.SignalConn(heartbeat_interval_s=0)
        assert sc.heartbeat_interval_s == 0


class TestSyncHeartbeatDefault:
    def test_default_interval(self):
        sc = sclient.SignalConn()
        assert sc.heartbeat_interval_s == 15.0

    def test_custom_interval(self):
        sc = sclient.SignalConn(heartbeat_interval_s=5.0)
        assert sc.heartbeat_interval_s == 5.0

    def test_disabled_when_zero(self):
        sc = sclient.SignalConn(heartbeat_interval_s=0)
        assert sc.heartbeat_interval_s == 0


class TestAsyncHeartbeatLoop:
    async def test_heartbeat_sends_ping(self):
        sc = aclient.SignalConn(heartbeat_interval_s=0.05)
        pings = []

        async def mock_send_cmd(cmd, key, arg):
            pings.append((cmd, key, arg))
            return "ok"

        sc._send_cmd = mock_send_cmd  # type: ignore[assignment]
        task = asyncio.create_task(sc._heartbeat_loop())
        await asyncio.sleep(0.15)
        task.cancel()
        await task  # should return cleanly (CancelledError caught internally)
        assert len(pings) >= 2
        assert all(p == ("ping", "_", "") for p in pings)

    async def test_heartbeat_stops_on_connection_error(self):
        sc = aclient.SignalConn(heartbeat_interval_s=0.05)

        async def mock_send_cmd(cmd, key, arg):
            raise ConnectionError("closed")

        sc._send_cmd = mock_send_cmd  # type: ignore[assignment]
        await sc._heartbeat_loop()  # should return, not raise

    async def test_heartbeat_not_started_when_zero(self):
        sc = aclient.SignalConn(
            server=("127.0.0.1", 1),
            connect_timeout_s=0.1,
            heartbeat_interval_s=0,
        )
        with pytest.raises((ConnectionRefusedError, OSError, asyncio.TimeoutError)):
            await sc.connect()
        assert sc._heartbeat_task is None


class TestSyncHeartbeatLoop:
    def test_heartbeat_sends_ping(self):
        sc = sclient.SignalConn(heartbeat_interval_s=0.05)
        pings = []

        def mock_send_cmd(cmd, key, arg):
            pings.append((cmd, key, arg))
            return "ok"

        sc._send_cmd = mock_send_cmd  # type: ignore[assignment]
        sc._heartbeat_stop = threading.Event()
        t = threading.Thread(target=sc._heartbeat_loop, daemon=True)
        t.start()
        import time

        time.sleep(0.15)
        sc._heartbeat_stop.set()
        t.join(timeout=2)
        assert len(pings) >= 2
        assert all(p == ("ping", "_", "") for p in pings)

    def test_heartbeat_stops_on_connection_error(self):
        sc = sclient.SignalConn(heartbeat_interval_s=0.05)

        def mock_send_cmd(cmd, key, arg):
            raise ConnectionError("closed")

        sc._send_cmd = mock_send_cmd  # type: ignore[assignment]
        sc._heartbeat_stop = threading.Event()
        sc._heartbeat_loop()  # should return, not raise

    def test_heartbeat_not_started_when_zero(self):
        sc = sclient.SignalConn(
            server=("127.0.0.1", 1),
            connect_timeout_s=0.1,
            heartbeat_interval_s=0,
        )
        with pytest.raises((ConnectionRefusedError, OSError)):
            sc.connect()
        assert sc._heartbeat_thread is None


# ===========================================================================
# Bug fix: sync _read_loop must not block on full _resp_queue
# ===========================================================================


class TestSyncRespQueueNonBlocking:
    def test_read_loop_does_not_block_on_full_resp_queue(self):
        sc = sclient.SignalConn()
        sc._resp_queue.put_nowait("stale")  # Fill maxsize=1 queue

        # rfile returns a non-signal line then EOF
        sc._rfile = io.StringIO("ok some_response\n")

        done = threading.Event()

        def run():
            sc._read_loop()
            done.set()

        t = threading.Thread(target=run, daemon=True)
        t.start()
        assert done.wait(timeout=2), "_read_loop blocked on full _resp_queue"

    def test_read_loop_routes_signals_and_responses(self):
        sc = sclient.SignalConn()
        sc._rfile = io.StringIO("sig events.login alice\nok 1\n")

        sc._read_loop()

        sig = sc._sig_queue.get_nowait()
        assert sig == Signal("events.login", "alice")
        resp = sc._resp_queue.get_nowait()
        assert resp == "ok 1"


# ===========================================================================
# Bug fix: async _send_cmd must clear _resp_future on early cancellation
# so the next caller's response is not mis-routed
# ===========================================================================


class TestAsyncSendCmdCancellation:
    async def test_resp_future_cleared_on_cancellation_during_drain(self):
        """If the awaiting task is cancelled before we reach the
        try/finally, _resp_future must still be reset to None — otherwise
        the in-flight response binds to an orphan future and the next
        caller's response gets mis-routed.
        """
        sc = aclient.SignalConn()
        # Stub writer so drain() awaits forever — simulates a slow flush
        # that gets cancelled before the response future is awaited.
        drain_waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        class _BlockingWriter:
            def write(self, data: bytes) -> None:
                pass

            async def drain(self) -> None:
                await drain_waiter

        sc._writer = _BlockingWriter()  # type: ignore[assignment]

        async def call_send_cmd():
            return await sc._send_cmd("listen", "x.>", "")

        task = asyncio.create_task(call_send_cmd())
        # Yield so call_send_cmd advances into the lock and assigns _resp_future.
        await asyncio.sleep(0.01)
        assert sc._resp_future is not None, "future should be set before cancel"

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # The orphan future must be cleared so a subsequent _send_cmd doesn't
        # collide with a late response from the cancelled command.
        assert sc._resp_future is None


# ===========================================================================
# Bug fix: dropped_signals counter must increment when queue is full
# ===========================================================================


class TestAsyncDroppedSignals:
    async def test_dropped_increments_on_full_queue(self):
        sc = aclient.SignalConn()
        for i in range(64):
            sc._sig_queue.put_nowait(Signal(f"ch.{i}", str(i)))
        assert sc._sig_queue.full()

        # Feed one more "sig ..." line; the read loop must drop and bump.
        reader = asyncio.StreamReader()
        reader.feed_data(b"sig overflow.channel payload\n")
        reader.feed_eof()
        sc._reader = reader

        await sc._read_loop()

        assert sc.dropped_signals == 1

    async def test_dropped_zero_when_consumer_keeps_up(self):
        sc = aclient.SignalConn()
        reader = asyncio.StreamReader()
        reader.feed_data(b"sig a hello\nsig b world\n")
        reader.feed_eof()
        sc._reader = reader

        await sc._read_loop()

        assert sc.dropped_signals == 0


class TestSyncDroppedSignals:
    def test_dropped_increments_on_full_queue(self):
        sc = sclient.SignalConn()
        for i in range(64):
            sc._sig_queue.put_nowait(Signal(f"ch.{i}", str(i)))
        assert sc._sig_queue.full()

        sc._rfile = io.StringIO("sig overflow.channel payload\n")
        sc._read_loop()

        assert sc.dropped_signals == 1

    def test_dropped_zero_when_consumer_keeps_up(self):
        sc = sclient.SignalConn()
        sc._rfile = io.StringIO("sig a hello\nsig b world\n")
        sc._read_loop()

        assert sc.dropped_signals == 0


class TestAsyncSigEmit:
    async def test_emit_returns_count(self, server_host_port):
        host, port = server_host_port
        reader, writer = await _open(host, port)
        try:
            n = await aclient.sig_emit(reader, writer, "test.channel", "hello")
            assert isinstance(n, int)
            assert n >= 0
        finally:
            writer.close()
            await writer.wait_closed()


class TestAsyncSignalConn:
    async def test_listen_emit_receive(self, server_host_port):
        host, port = server_host_port
        async with aclient.SignalConn(server=(host, port)) as listener:
            await listener.listen("test.signals.>")

            # Emit from a separate connection
            async with aclient.SignalConn(server=(host, port)) as emitter:
                n = await emitter.emit("test.signals.hello", "world")
                assert n >= 1

            sig = await asyncio.wait_for(listener.signals.get(), timeout=5)
            assert sig is not None
            assert sig.channel == "test.signals.hello"
            assert sig.payload == "world"

    async def test_unlisten_stops_delivery(self, server_host_port):
        host, port = server_host_port
        async with aclient.SignalConn(server=(host, port)) as listener:
            await listener.listen("test.unsub.>")
            await listener.unlisten("test.unsub.>")

            async with aclient.SignalConn(server=(host, port)) as emitter:
                await emitter.emit("test.unsub.msg", "data")

            # Give a moment for any signal to arrive (it shouldn't)
            await asyncio.sleep(0.1)
            assert listener.signals.empty()

    async def test_wildcard_star_match(self, server_host_port):
        host, port = server_host_port
        async with aclient.SignalConn(server=(host, port)) as listener:
            await listener.listen("test.wild.*.event")

            async with aclient.SignalConn(server=(host, port)) as emitter:
                await emitter.emit("test.wild.user.event", "p1")

            sig = await asyncio.wait_for(listener.signals.get(), timeout=5)
            assert sig is not None
            assert sig.channel == "test.wild.user.event"

    async def test_queue_group(self, server_host_port):
        host, port = server_host_port
        async with (
            aclient.SignalConn(server=(host, port)) as a,
            aclient.SignalConn(server=(host, port)) as b,
            aclient.SignalConn(server=(host, port)) as emitter,
        ):
            await a.listen("test.qg.>", group="workers")
            await b.listen("test.qg.>", group="workers")

            total_sent = 4
            for i in range(total_sent):
                await emitter.emit("test.qg.job", str(i))

            await asyncio.sleep(0.2)
            got_a = 0
            got_b = 0
            while not a.signals.empty():
                item = a.signals.get_nowait()
                if item is not None:
                    got_a += 1
            while not b.signals.empty():
                item = b.signals.get_nowait()
                if item is not None:
                    got_b += 1

            # Each signal delivered to exactly one member
            assert got_a + got_b == total_sent
            # Both should get at least one (round-robin)
            assert got_a >= 1
            assert got_b >= 1

    async def test_async_for_iteration(self, server_host_port):
        host, port = server_host_port
        async with aclient.SignalConn(server=(host, port)) as listener:
            await listener.listen("test.iter.>")

            async with aclient.SignalConn(server=(host, port)) as emitter:
                for i in range(3):
                    await emitter.emit("test.iter.msg", str(i))

            received = []
            async for sig in listener:
                received.append(sig.payload)
                if len(received) >= 3:
                    break
            assert received == ["0", "1", "2"]

    async def test_close_ends_iteration(self, server_host_port):
        host, port = server_host_port
        sc = aclient.SignalConn(server=(host, port))
        await sc.connect()
        await sc.listen("test.close.>")

        async def close_soon():
            await asyncio.sleep(0.2)
            await sc.aclose()

        asyncio.create_task(close_soon())
        received = []
        async for sig in sc:
            received.append(sig)
        # iteration should end cleanly after close
        assert isinstance(received, list)


# ===========================================================================
# Sync integration tests (require running dflockd server)
# ===========================================================================


class TestSyncSigEmit:
    def test_emit_returns_count(self, server_host_port):
        host, port = server_host_port
        sock, rfile = _sync_connect(host, port)
        try:
            n = sclient.sig_emit(sock, rfile, "test.sync.channel", "hello")
            assert isinstance(n, int)
            assert n >= 0
        finally:
            rfile.close()
            sock.close()


class TestSyncSignalConn:
    def test_listen_emit_receive(self, server_host_port):
        host, port = server_host_port
        with sclient.SignalConn(server=(host, port)) as listener:
            listener.listen("test.sync.signals.>")

            with sclient.SignalConn(server=(host, port)) as emitter:
                n = emitter.emit("test.sync.signals.hello", "world")
                assert n >= 1

            sig = listener.signals.get(timeout=5)
            assert sig is not None
            assert sig.channel == "test.sync.signals.hello"
            assert sig.payload == "world"

    def test_unlisten_stops_delivery(self, server_host_port):
        host, port = server_host_port
        with sclient.SignalConn(server=(host, port)) as listener:
            listener.listen("test.sync.unsub.>")
            listener.unlisten("test.sync.unsub.>")

            with sclient.SignalConn(server=(host, port)) as emitter:
                emitter.emit("test.sync.unsub.msg", "data")

            import time

            time.sleep(0.1)
            assert listener.signals.empty()

    def test_for_iteration(self, server_host_port):
        host, port = server_host_port
        with sclient.SignalConn(server=(host, port)) as listener:
            listener.listen("test.sync.iter.>")

            with sclient.SignalConn(server=(host, port)) as emitter:
                for i in range(3):
                    emitter.emit("test.sync.iter.msg", str(i))

            received = []
            for sig in listener:
                received.append(sig.payload)
                if len(received) >= 3:
                    break
            assert received == ["0", "1", "2"]

    def test_close_ends_iteration(self, server_host_port):
        host, port = server_host_port
        sc = sclient.SignalConn(server=(host, port))
        sc.connect()
        sc.listen("test.sync.close.>")

        def close_soon():
            import time

            time.sleep(0.2)
            sc.close()

        t = threading.Thread(target=close_soon)
        t.start()
        received = []
        for sig in sc:
            received.append(sig)
        t.join(timeout=5)
        assert isinstance(received, list)
