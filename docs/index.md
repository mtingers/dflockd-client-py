# dflockd-client

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a lightweight distributed lock server with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- Async and sync clients with automatic background lease renewal
- Distributed locks and counting semaphores with FIFO ordering
- Signals (pub/sub) with NATS-style wildcards and queue groups
- Two-phase acquisition, multi-server sharding, TLS, authentication
- Zero dependencies — pure Python 3.12+

## Quick example

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock("my-key") as lock:
    print(f"acquired: {lock.token}")
    # critical section — lease auto-renews in background
```

```python
from dflockd_client import SyncDistributedSemaphore

with SyncDistributedSemaphore("pool", limit=3) as sem:
    # up to 3 concurrent holders
    ...
```

```python
from dflockd_client import SyncSignalConn

with SyncSignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
        break
```

## Getting started

- [Installation](getting-started/installation.md) — install with pip or uv
- [Quick Start](getting-started/quickstart.md) — acquire your first lock
- [Examples](getting-started/examples.md) — async, sync, FIFO ordering, signals, sharding, and more
