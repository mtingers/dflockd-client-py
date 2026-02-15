# dflockd-client

<!--toc:start-->
- [dflockd-client](#dflockd-client)
  - [Installation](#installation)
  - [Quick start](#quick-start)
    - [Async client](#async-client)
    - [Sync client](#sync-client)
    - [Manual acquire/release](#manual-acquirerelease)
    - [Two-phase lock acquisition](#two-phase-lock-acquisition)
    - [Parameters](#parameters)
  - [Multi-server sharding](#multi-server-sharding)
<!--toc:end-->

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a lightweight distributed lock server with FIFO ordering, automatic lease expiry, and background renewal.

## Installation

```bash
pip install dflockd-client
```

Or with uv:

```bash
uv add dflockd-client
```

## Quick start

### Async client

```python
import asyncio
from dflockd_client.client import DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(lock.token, lock.lease)
        # critical section — lease auto-renews in background

asyncio.run(main())
```

### Sync client

```python
from dflockd_client.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(lock.token, lock.lease)
    # critical section — lease auto-renews in background thread
```

### Manual acquire/release

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

### Two-phase lock acquisition

The `enqueue()` / `wait()` methods split lock acquisition into two steps, allowing you to notify an external system after joining the queue but before blocking:

```python
from dflockd_client.sync_client import DistributedLock

lock = DistributedLock("my-key")
status = lock.enqueue()       # join queue, returns "acquired" or "queued"
notify_external_system()      # your application logic here
if lock.wait(timeout_s=10):   # block until granted (no-op if already acquired)
    try:
        pass  # critical section
    finally:
        lock.release()
```

Async equivalent:

```python
lock = DistributedLock("my-key")
status = await lock.enqueue()
await notify_external_system()
if await lock.wait(timeout_s=10):
    try:
        pass  # critical section
    finally:
        await lock.release()
```

### Parameters

| Parameter           | Default                 | Description                                                             |
| ------------------- | ----------------------- | ----------------------------------------------------------------------- |
| `key`               | _(required)_            | Lock name                                                               |
| `acquire_timeout_s` | `10`                    | Seconds to wait for lock acquisition                                    |
| `lease_ttl_s`       | `None` (server default) | Lease duration in seconds                                               |
| `servers`           | `[("127.0.0.1", 6388)]` | List of `(host, port)` tuples                                           |
| `sharding_strategy` | `stable_hash_shard`     | `Callable[[str, int], int]` — maps `(key, num_servers)` to server index |
| `renew_ratio`       | `0.5`                   | Renew at `lease * ratio` seconds                                        |

## Multi-server sharding

When running multiple dflockd instances, the client can distribute keys across servers using consistent hashing. Each key always routes to the same server.

```python
from dflockd_client.sync_client import DistributedLock

servers = [("server1", 6388), ("server2", 6388), ("server3", 6388)]

with DistributedLock("my-key", servers=servers) as lock:
    print(lock.token, lock.lease)
```

The default strategy uses `zlib.crc32` for stable, deterministic hashing. You can provide a custom strategy:

```python
from dflockd_client.sync_client import DistributedLock

def my_strategy(key: str, num_servers: int) -> int:
    """Route all keys to the first server."""
    return 0

with DistributedLock("my-key", servers=servers, sharding_strategy=my_strategy) as lock:
    pass
```
