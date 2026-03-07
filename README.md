# dflockd-client

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a lightweight distributed lock server with FIFO ordering, automatic lease expiry, and background renewal.

[Documentation](https://mtingers.github.io/dflockd-client-py/) · [Changelog](CHANGELOG.md)

## Features

- Async and sync clients with automatic background lease renewal
- Distributed locks and counting semaphores with FIFO ordering
- Signals (pub/sub) with NATS-style wildcards and queue groups
- Two-phase acquisition (enqueue + wait)
- Multi-server sharding with consistent hashing
- TLS and token-based authentication
- Zero dependencies — pure Python 3.12+

## Installation

```bash
pip install dflockd-client
# or: uv add dflockd-client
```

## Quick start

### Lock

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock("my-key") as lock:
    print(f"acquired: {lock.token}")
    # critical section — lease auto-renews in background
```

Async:

```python
import asyncio
from dflockd_client import AsyncDistributedLock

async def main():
    async with AsyncDistributedLock("my-key") as lock:
        print(f"acquired: {lock.token}")

asyncio.run(main())
```

### Semaphore

```python
from dflockd_client import SyncDistributedSemaphore

with SyncDistributedSemaphore("pool", limit=3) as sem:
    print(f"acquired: {sem.token}")
    # up to 3 concurrent holders
```

### Signals

```python
from dflockd_client import SyncSignalConn

with SyncSignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
        break
```

### Authentication and TLS

```python
import ssl

ctx = ssl.create_default_context()

with SyncDistributedLock("my-key", auth_token="secret", ssl_context=ctx) as lock:
    ...
```

### Multi-server sharding

```python
servers = [("server1", 6388), ("server2", 6388), ("server3", 6388)]

with SyncDistributedLock("my-key", servers=servers) as lock:
    ...  # key deterministically routes to the same server
```

For detailed guides, API reference, and more examples, see the [documentation](https://mtingers.github.io/dflockd-client-py/).
