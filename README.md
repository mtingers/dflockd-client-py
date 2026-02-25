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
  - [TLS](#tls)
  - [Semaphores](#semaphores)
    - [Parameters](#parameters-1)
  - [Stats](#stats)
  - [Multi-server sharding](#multi-server-sharding)
          <!--toc:end-->

A Python client library for [dflockd](https://github.com/mtingers/dflockd) — a
lightweight distributed lock server with FIFO ordering, automatic lease expiry,
and background renewal.

[Read the docs here](https://mtingers.github.io/dflockd-client-py/)

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

> **Tip:** You can also use the top-level import alias:
> `from dflockd_client import SyncDistributedLock` (or `AsyncDistributedLock` for async).

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
| `ssl_context`       | `None`                  | `ssl.SSLContext` for TLS connections. `None` uses plain TCP              |
| `auth_token`        | `None`                  | Auth token for servers started with `--auth-token`. `None` skips auth    |
| `connect_timeout_s` | `10`                    | Seconds to wait for the TCP connection to the server                     |

## Authentication

When the dflockd server is started with `--auth-token`, pass the token to authenticate:

```python
from dflockd_client.sync_client import DistributedLock

with DistributedLock("my-key", auth_token="mysecret") as lock:
    print(lock.token, lock.lease)
```

Async equivalent:

```python
from dflockd_client.client import DistributedLock

async with DistributedLock("my-key", auth_token="mysecret") as lock:
    print(lock.token, lock.lease)
```

Both `DistributedLock` and `DistributedSemaphore` accept `auth_token` in the async and sync clients. A `PermissionError` is raised if the token is invalid.

## TLS

To connect to a TLS-enabled dflockd server, pass an `ssl.SSLContext`:

```python
import ssl
from dflockd_client.sync_client import DistributedLock

ctx = ssl.create_default_context()  # uses system CA bundle
# or: ctx = ssl.create_default_context(cafile="/path/to/ca.pem")

with DistributedLock("my-key", ssl_context=ctx) as lock:
    print(lock.token, lock.lease)
```

Async equivalent:

```python
import ssl
from dflockd_client.client import DistributedLock

ctx = ssl.create_default_context()

async with DistributedLock("my-key", ssl_context=ctx) as lock:
    print(lock.token, lock.lease)
```

Both `DistributedLock` and `DistributedSemaphore` accept `ssl_context` in the async and sync clients.

## Semaphores

`DistributedSemaphore` allows up to N concurrent holders per key, using the same API patterns as `DistributedLock`:

```python
from dflockd_client.sync_client import DistributedSemaphore

# Allow up to 3 concurrent workers on this key
with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(sem.token, sem.lease)
    # critical section — up to 3 holders at once
```

Async equivalent:

```python
from dflockd_client.client import DistributedSemaphore

async with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(sem.token, sem.lease)
```

Manual acquire/release and two-phase (`enqueue()` / `wait()`) work the same as locks.

### Parameters

| Parameter           | Default                 | Description                                                             |
| ------------------- | ----------------------- | ----------------------------------------------------------------------- |
| `key`               | _(required)_            | Semaphore name                                                          |
| `limit`             | _(required)_            | Maximum concurrent holders                                              |
| `acquire_timeout_s` | `10`                    | Seconds to wait for acquisition                                         |
| `lease_ttl_s`       | `None` (server default) | Lease duration in seconds                                               |
| `servers`           | `[("127.0.0.1", 6388)]` | List of `(host, port)` tuples                                           |
| `sharding_strategy` | `stable_hash_shard`     | `Callable[[str, int], int]` — maps `(key, num_servers)` to server index |
| `renew_ratio`       | `0.5`                   | Renew at `lease * ratio` seconds                                        |
| `ssl_context`       | `None`                  | `ssl.SSLContext` for TLS connections. `None` uses plain TCP              |
| `auth_token`        | `None`                  | Auth token for servers started with `--auth-token`. `None` skips auth    |
| `connect_timeout_s` | `10`                    | Seconds to wait for the TCP connection to the server                     |

## Stats

Query server state (connections, held locks, active semaphores) using the low-level `stats()` function:

```python
import asyncio
from dflockd_client.client import stats

async def main():
    reader, writer = await asyncio.open_connection("127.0.0.1", 6388)
    result = await stats(reader, writer)
    print(result)
    # {'connections': 1, 'locks': [], 'semaphores': [], 'idle_locks': [], 'idle_semaphores': []}
    writer.close()
    await writer.wait_closed()

asyncio.run(main())
```

Sync equivalent:

```python
import socket
from dflockd_client.sync_client import stats

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")
result = stats(sock, rfile)
print(result)
rfile.close()
sock.close()
```

Returns a dict with `connections`, `locks`, `semaphores`, `idle_locks`, and `idle_semaphores`.

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
