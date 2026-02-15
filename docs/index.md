# dflockd-client

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a lightweight distributed lock server with FIFO ordering, automatic lease expiry, and background renewal.

## Features

- **Async and sync clients** — choose the client that fits your application
- **Automatic lease renewal** — both clients auto-renew leases in the background
- **Two-phase lock acquisition** — split enqueue and wait to notify external systems between joining the queue and blocking
- **Multi-server sharding** — distribute keys across multiple servers with consistent hashing
- **Zero dependencies** — pure Python 3.12+ using only the standard library
- **Context manager support** — acquire on entry, release on exit

## Quick example

```python
from dflockd_client.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"acquired: {lock.token}")
    # critical section — lease auto-renews in background
```

## Getting started

- [Installation](getting-started/installation.md) — install dflockd-client with pip or uv
- [Quick Start](getting-started/quickstart.md) — acquire your first lock
- [Examples](getting-started/examples.md) — async, sync, FIFO ordering, and multi-server demos
