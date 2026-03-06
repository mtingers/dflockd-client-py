# dflockd-client

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a lightweight distributed lock server with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Async and sync clients** — choose the client that fits your application
- **Locks and semaphores** — mutual exclusion with `DistributedLock`, bounded concurrency with `DistributedSemaphore`
- **Automatic lease renewal** — both clients auto-renew leases in the background
- **Two-phase acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Multi-server sharding** — distribute keys across multiple servers with consistent hashing
- **Signals (pub/sub)** — publish and subscribe to named channels with NATS-style wildcards and queue groups via `SignalConn`
- **Server stats** — query connections, held locks, and active semaphores via `stats()`
- **Robustness** — `ResourceWarning` safety net for unclosed clients, 1 MiB response size limits, hardened renew loop with staleness checks
- **Zero dependencies** — pure Python 3.12+ using only the standard library
- **Context manager support** — acquire on entry, release on exit

## Quick example

```python
from dflockd_client.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"acquired: {lock.token}")
    # critical section — lease auto-renews in background
```

## Semaphore example

```python
from dflockd_client.sync_client import DistributedSemaphore

# Allow up to 3 concurrent holders
with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(f"acquired: {sem.token}")
    # up to 3 workers can hold this simultaneously
```

## Signal example

```python
from dflockd_client.sync_client import SignalConn

with SignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    # sc.emit("events.user.login", "alice")  # from another connection
    for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
        break
```

## Getting started

- [Installation](getting-started/installation.md) — install dflockd-client with pip or uv
- [Quick Start](getting-started/quickstart.md) — acquire your first lock
- [Examples](getting-started/examples.md) — async, sync, FIFO ordering, and multi-server demos
