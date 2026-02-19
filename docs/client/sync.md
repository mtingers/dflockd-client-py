# Sync Client

The sync client uses standard `socket` and `threading` for blocking lock and semaphore operations with automatic background lease renewal. No asyncio required.

```python
from dflockd_client.sync_client import DistributedLock, DistributedSemaphore
```

## Context manager

The recommended way to use the client:

```python
from dflockd_client.sync_client import DistributedLock

with DistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"token={lock.token} lease={lock.lease}")
    # critical section
```

If the lock cannot be acquired within the timeout, a `TimeoutError` is raised.

## Manual acquire/release

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)
if lock.acquire():
    try:
        # critical section
        pass
    finally:
        lock.release()
```

`acquire()` returns `False` on timeout instead of raising.

## Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `key` | `str` | *(required)* | Lock name |
| `acquire_timeout_s` | `int` | `10` | Seconds to wait for acquisition |
| `lease_ttl_s` | `int \| None` | `None` | Lease duration (seconds). `None` uses server default |
| `servers` | `list[tuple[str, int]]` | `[("127.0.0.1", 6388)]` | Server addresses |
| `sharding_strategy` | `ShardingStrategy` | `stable_hash_shard` | Key-to-server mapping function |
| `renew_ratio` | `float` | `0.5` | Renew at `lease * ratio` seconds |
| `ssl_context` | `ssl.SSLContext \| None` | `None` | TLS context. `None` uses plain TCP |

## TLS

To connect to a TLS-enabled server, pass an `ssl.SSLContext`:

```python
import ssl
from dflockd_client.sync_client import DistributedLock

ctx = ssl.create_default_context()  # uses system CA bundle
# or: ctx = ssl.create_default_context(cafile="/path/to/ca.pem")

with DistributedLock("my-key", ssl_context=ctx) as lock:
    print(f"token={lock.token}")
```

The same `ssl_context` parameter is available on `DistributedSemaphore`.

## Attributes

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | The lock token (UUID hex). `None` if not held |
| `lease` | `int` | Lease duration in seconds as reported by the server |

## Background renewal

Once acquired, a daemon thread sends renew requests at `lease * renew_ratio` intervals. If renewal fails, the client logs an error and sets `token = None`.

The renewal thread is stopped automatically on `release()`, context manager exit, or `close()`.

## Cleanup

Always call `release()` or `close()` when using manual acquire:

```python
lock = DistributedLock("my-key")
try:
    if lock.acquire():
        # work
        lock.release()
finally:
    lock.close()
```

## Two-phase lock acquisition

The `enqueue()` / `wait()` methods split lock acquisition into two steps. This lets you notify an external system after joining the queue but before blocking:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)

# Step 1: join the queue (returns immediately)
status = lock.enqueue()  # "acquired" or "queued"

# Step 2: notify external system
notify_external_system(status)

# Step 3: block until granted (no-op if already acquired)
if lock.wait(timeout_s=10):
    try:
        # critical section
        pass
    finally:
        lock.release()
```

**`enqueue()`** connects to the server and sends the `e` command. Returns `"acquired"` if the lock was free (fast path) or `"queued"` if there are other holders/waiters. On fast-path acquire, the renewal thread starts immediately.

**`wait(timeout_s=None)`** sends the `w` command and blocks until the lock is granted. Returns `True` on success, `False` on timeout. If the lock was already acquired during `enqueue()`, returns `True` immediately without contacting the server. Uses `acquire_timeout_s` if `timeout_s` is not provided.

## Low-level functions

Direct protocol functions are also available:

```python
import socket
from dflockd_client.sync_client import acquire, release, renew

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")

token, lease = acquire(sock, rfile, "my-key", acquire_timeout_s=10)
remaining = renew(sock, rfile, "my-key", token)
release(sock, rfile, "my-key", token)

rfile.close()
sock.close()
```

The two-phase functions are also available at the low level:

```python
from dflockd_client.sync_client import enqueue, wait, release

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")

status, token, lease = enqueue(sock, rfile, "my-key")
# status is "acquired" or "queued"

if status == "queued":
    token, lease = wait(sock, rfile, "my-key", wait_timeout_s=10)

release(sock, rfile, "my-key", token)

rfile.close()
sock.close()
```

## Semaphores

`DistributedSemaphore` allows up to N concurrent holders on the same key. It has the same API as `DistributedLock` plus a required `limit` parameter.

### Context manager

```python
from dflockd_client.sync_client import DistributedSemaphore

with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(f"token={sem.token} lease={sem.lease}")
    # up to 3 holders at once
```

### Manual acquire/release

```python
sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10)
if sem.acquire():
    try:
        pass  # critical section
    finally:
        sem.release()
```

### Two-phase semaphore acquisition

```python
sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10)

status = sem.enqueue()  # "acquired" or "queued"
notify_external_system(status)

if sem.wait(timeout_s=10):
    try:
        pass  # critical section
    finally:
        sem.release()
```

### Semaphore parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `key` | `str` | *(required)* | Semaphore name |
| `limit` | `int` | *(required)* | Maximum concurrent holders |
| `acquire_timeout_s` | `int` | `10` | Seconds to wait for acquisition |
| `lease_ttl_s` | `int \| None` | `None` | Lease duration (seconds). `None` uses server default |
| `servers` | `list[tuple[str, int]]` | `[("127.0.0.1", 6388)]` | Server addresses |
| `sharding_strategy` | `ShardingStrategy` | `stable_hash_shard` | Key-to-server mapping function |
| `renew_ratio` | `float` | `0.5` | Renew at `lease * ratio` seconds |
| `ssl_context` | `ssl.SSLContext \| None` | `None` | TLS context. `None` uses plain TCP |

### Semaphore low-level functions

```python
import socket
from dflockd_client.sync_client import sem_acquire, sem_release, sem_renew

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")

token, lease = sem_acquire(sock, rfile, "my-key", acquire_timeout_s=10, limit=3)
remaining = sem_renew(sock, rfile, "my-key", token)
sem_release(sock, rfile, "my-key", token)

rfile.close()
sock.close()
```

## Stats

Query the server for current state using the low-level `stats()` function:

```python
import socket
from dflockd_client.sync_client import stats

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")
result = stats(sock, rfile)
print(result)
# {'connections': 1, 'locks': [], 'semaphores': [], 'idle_locks': [], 'idle_semaphores': []}
rfile.close()
sock.close()
```

Returns a dict with:

| Field | Type | Description |
|---|---|---|
| `connections` | `int` | Number of connected TCP clients |
| `locks` | `list[dict]` | Held locks with `key`, `owner_conn_id`, `lease_expires_in_s`, `waiters` |
| `semaphores` | `list[dict]` | Active semaphores with `key`, `limit`, `holders`, `waiters` |
| `idle_locks` | `list` | Unused lock entries |
| `idle_semaphores` | `list` | Unused semaphore entries |

## Async vs sync

| | Async | Sync |
|---|---|---|
| Import | `dflockd_client.client` | `dflockd_client.sync_client` |
| Context manager | `async with` | `with` |
| Renewal | `asyncio.Task` | `threading.Thread` (daemon) |
| Cleanup | `await lock.aclose()` | `lock.close()` |
| Best for | asyncio applications, high concurrency | Scripts, threads, simple applications |
