# Quick Start

## Prerequisites

You need a running [dflockd](https://github.com/mtingers/dflockd) server. By default, the client connects to `127.0.0.1:6388`.

## Acquire a lock

### Async client

```python
import asyncio
from dflockd_client.client import DistributedLock
# or: from dflockd_client import AsyncDistributedLock as DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")
        # critical section — lease auto-renews in background

asyncio.run(main())
```

### Sync client

```python
from dflockd_client.sync_client import DistributedLock
# or: from dflockd_client import SyncDistributedLock as DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"token={lock.token} lease={lock.lease}")
    # critical section — lease auto-renews in background thread
```

## Manual acquire/release

Both clients support explicit `acquire()` / `release()` outside of a context manager:

```python
from dflockd_client.sync_client import DistributedLock

lock = DistributedLock("my-key")
if lock.acquire():
    try:
        pass  # critical section
    finally:
        lock.release()
```

## Acquire a semaphore

Semaphores allow up to N concurrent holders on the same key.

### Async client

```python
import asyncio
from dflockd_client.client import DistributedSemaphore

async def main():
    async with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
        print(f"token={sem.token} lease={sem.lease}")
        # up to 3 holders at once

asyncio.run(main())
```

### Sync client

```python
from dflockd_client.sync_client import DistributedSemaphore

with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(f"token={sem.token} lease={sem.lease}")
    # up to 3 holders at once
```

## Subscribe to signals

Use `SignalConn` for pub/sub messaging:

### Async client

```python
import asyncio
from dflockd_client.client import SignalConn

async def main():
    async with SignalConn(server=("127.0.0.1", 6388)) as sc:
        await sc.listen("events.>")
        async for sig in sc:
            print(f"{sig.channel}: {sig.payload}")

asyncio.run(main())
```

### Sync client

```python
from dflockd_client.sync_client import SignalConn

with SignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
```

See [Examples](examples.md) for stats queries, authentication, TLS, sharding, and more advanced patterns. See [Architecture](../guide/architecture.md) for how the client works internally.
