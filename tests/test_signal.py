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
# Async integration tests (require running dflockd server)
# ===========================================================================


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
