# dflockd-client

A Python client for [dflockd](https://github.com/mtingers/dflockd) — a
distributed FIFO lock and counting-semaphore server.

## Features

- Sync and async clients with the same API surface
- Distributed locks and counting semaphores with FIFO ordering
- Single-phase `acquire` and two-phase `enqueue` + `wait`
- Background lease renewal so you can hold a lock as long as the work takes
- Multi-server sharding (CRC-32; cross-language compatible)
- TLS and shared-secret authentication
- Zero runtime dependencies; Python 3.12+

## At a glance

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock("my-key") as lock:
    # critical section — lease auto-renews in the background
    print(f"acquired: {lock.token}")
```

```python
from dflockd_client import SyncDistributedSemaphore

with SyncDistributedSemaphore("pool", limit=3) as sem:
    # up to 3 holders at once
    ...
```

## Where to go next

- [Installation](getting-started/installation.md) — pip / uv
- [Quick Start](getting-started/quickstart.md) — first lock and semaphore
- [Examples](getting-started/examples.md) — FIFO ordering, two-phase, auth, TLS, sharding
- [Sync Client](client/sync.md) / [Async Client](client/async.md) — full method reference
- [Architecture](guide/architecture.md) — how it works internally
- [Sharding](guide/sharding.md) — routing keys across servers
- [API Reference](api/python.md) — every public symbol
