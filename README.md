# dflockd-client

A Python client for [dflockd](https://github.com/mtingers/dflockd) — a
distributed FIFO lock and counting-semaphore server.

[Documentation](https://mtingers.github.io/dflockd-client-py/) ·
[Changelog](CHANGELOG.md)

## Features

- Sync and async clients — pick the one that matches your runtime
- Distributed locks and counting semaphores with FIFO ordering
- Single-phase `acquire` and two-phase `enqueue` + `wait`
- Background lease renewal — call `acquire`, hold for as long as you need
- Grant tokens carry a monotonic fence prefix — usable as fencing tokens
- Multi-server sharding (deterministic CRC-32; matches the Go and TypeScript clients)
- TLS and shared-secret authentication
- Zero runtime dependencies; Python 3.12+

## Install

```bash
pip install dflockd-client
# or
uv add dflockd-client
```

## Quick start

A running [dflockd](https://github.com/mtingers/dflockd) server on
`127.0.0.1:6388` is the only prerequisite.

### Lock

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock("my-key") as lock:
    # critical section — lease auto-renews in a daemon thread
    print(f"acquired: {lock.token}")
```

```python
import asyncio
from dflockd_client import AsyncDistributedLock

async def main():
    async with AsyncDistributedLock("my-key") as lock:
        print(f"acquired: {lock.token}")

asyncio.run(main())
```

### Semaphore

Up to `limit` concurrent holders on the same key:

```python
from dflockd_client import SyncDistributedSemaphore

with SyncDistributedSemaphore("pool", limit=3) as sem:
    print(f"slot: {sem.token}")
```

### Authentication and TLS

```python
import ssl
from dflockd_client import SyncDistributedLock

with SyncDistributedLock(
    "my-key",
    auth_token="shared-secret",
    ssl_context=ssl.create_default_context(),
) as lock:
    ...
```

### Multi-server sharding

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock(
    "my-key",
    servers=[("a", 6388), ("b", 6388), ("c", 6388)],
) as lock:
    # the same key always routes to the same server
    ...
```

### Fencing tokens

Every grant returns a 32-char hex token whose first 16 hex chars are a
monotonic `uint64` (big-endian) that strictly increases on every grant from
a server. `fence_from_token` parses it, so the token doubles as a
[fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
a downstream resource stores the highest fence it has seen for a key and
rejects any write whose fence compares less.

```python
from dflockd_client import SyncDistributedLock, fence_from_token

with SyncDistributedLock("row:42") as lock:
    fence = fence_from_token(lock.token)  # int — pass to your DB / blob store
```

See the [docs](https://mtingers.github.io/dflockd-client-py/) for two-phase
acquisition, exception handling, low-level transport, and more.
