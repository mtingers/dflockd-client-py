import os
import socket

import pytest


def _server_available(host: str, port: int) -> bool:
    """Check if a dflockd server is reachable."""
    try:
        sock = socket.create_connection((host, port), timeout=1)
        sock.close()
        return True
    except OSError:
        return False


@pytest.fixture()
def server_port():
    """Return the port of a running dflockd server.

    Configure with DFLOCKD_TEST_HOST and DFLOCKD_TEST_PORT environment
    variables. Defaults to 127.0.0.1:6388. Tests requiring this fixture
    are skipped if the server is not reachable.
    """
    host = os.environ.get("DFLOCKD_TEST_HOST", "127.0.0.1")
    port = int(os.environ.get("DFLOCKD_TEST_PORT", "6388"))
    if not _server_available(host, port):
        pytest.skip(f"dflockd server not available at {host}:{port}")
    return port
