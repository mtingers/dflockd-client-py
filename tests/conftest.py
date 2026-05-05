"""Shared fixtures.

Integration tests use the ``server_host_port`` fixture, which checks that a
dflockd server is reachable and skips the test otherwise. Override the
location with ``DFLOCKD_TEST_HOST`` / ``DFLOCKD_TEST_PORT``.
"""

import os
import socket

import pytest


def _server_available(host: str, port: int) -> bool:
    try:
        sock = socket.create_connection((host, port), timeout=1)
        sock.close()
        return True
    except OSError:
        return False


@pytest.fixture()
def server_host_port() -> tuple[str, int]:
    host = os.environ.get("DFLOCKD_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("DFLOCKD_TEST_PORT", "6388"))
    if not _server_available(host, port):
        pytest.skip(f"dflockd server not available at {host}:{port}")
    return host, port
