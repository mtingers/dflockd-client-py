# Async Client

The async client uses `asyncio` for non-blocking lock, semaphore, and signal operations with automatic background lease renewal.

```python
from dflockd_client.client import DistributedLock, DistributedSemaphore, SignalConn
```

**Alternative top-level imports** (equivalent):

```python
from dflockd_client import AsyncDistributedLock, AsyncDistributedSemaphore, AsyncSignalConn
```

## Context manager

The recommended way to use the client. The lock is acquired on entry and released on exit:

```python
import asyncio
from dflockd_client.client import DistributedLock

async def main():
    async with DistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")
        # critical section

asyncio.run(main())
```

If the lock cannot be acquired within the timeout, a `TimeoutError` is raised.

## Manual acquire/release

For cases where a context manager doesn't fit:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)
acquired = await lock.acquire()
if acquired:
    try:
        # critical section
        pass
    finally:
        await lock.release()
```

`acquire()` returns `False` on timeout instead of raising.

## Parameters

`key` is the only positional parameter. All others are keyword-only.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `key` | `str` | *(required)* | Lock name |
| `acquire_timeout_s` | `int` | `10` | Seconds to wait for acquisition |
| `lease_ttl_s` | `int \| None` | `None` | Lease duration (seconds). `None` uses server default |
| `servers` | `list[tuple[str, int]]` | `[("127.0.0.1", 6388)]` | Server addresses |
| `sharding_strategy` | `ShardingStrategy` | `stable_hash_shard` | Key-to-server mapping function |
| `renew_ratio` | `float` | `0.5` | Renew at `lease * ratio` seconds |
| `ssl_context` | `ssl.SSLContext \| None` | `None` | TLS context. `None` uses plain TCP |
| `auth_token` | `str \| None` | `None` | Auth token for servers started with `--auth-token`. `None` skips auth |
| `connect_timeout_s` | `float` | `10` | Seconds to wait for the TCP connection |

## Authentication

When the dflockd server is started with `--auth-token`, pass the token to authenticate:

```python
from dflockd_client.client import DistributedLock

async with DistributedLock("my-key", auth_token="mysecret") as lock:
    print(f"token={lock.token}")
```

The same `auth_token` parameter is available on `DistributedSemaphore`. A `PermissionError` is raised if the token is invalid.

## TLS

To connect to a TLS-enabled server, pass an `ssl.SSLContext`:

```python
import ssl
from dflockd_client.client import DistributedLock

ctx = ssl.create_default_context()  # uses system CA bundle
# or: ctx = ssl.create_default_context(cafile="/path/to/ca.pem")

async with DistributedLock("my-key", ssl_context=ctx) as lock:
    print(f"token={lock.token}")
```

The same `ssl_context` parameter is available on `DistributedSemaphore`.

## Attributes

After acquiring a lock, these attributes are available:

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | The lock token (UUID hex). `None` if not held |
| `lease` | `int` | Lease duration in seconds as reported by the server |

## Background renewal

Once a lock is acquired, the client starts an `asyncio.Task` that sends renew requests at `lease * renew_ratio` intervals. If renewal fails (server unreachable, lease already expired), the client logs an error and the renewal loop exits. The lease will eventually expire server-side.

The renewal task includes staleness checks — if the connection is replaced (e.g. after a reconnect), the old renewal task detects the identity mismatch and exits cleanly. If the server returns a zero-length lease, the renewal loop falls back to a 30-second interval instead of spinning aggressively.

The renewal task is cancelled automatically on `release()`, context manager exit, or `aclose()`.

## Cleanup

If you use manual `acquire()`, always call `release()` or `aclose()` to clean up the connection:

```python
lock = DistributedLock("my-key")
try:
    if await lock.acquire():
        # work
        await lock.release()
finally:
    await lock.aclose()
```

If a client is garbage collected without being properly closed, `__del__` will close the underlying transport and emit a `ResourceWarning` to help catch leaked connections during development.

## Two-phase lock acquisition

The `enqueue()` / `wait()` methods split lock acquisition into two steps. This lets you notify an external system after joining the queue but before blocking:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)

# Step 1: join the queue (returns immediately)
status = await lock.enqueue()  # "acquired" or "queued"

# Step 2: notify external system
await notify_external_system(status)

# Step 3: block until granted (no-op if already acquired)
if await lock.wait(timeout_s=10):
    try:
        # critical section
        pass
    finally:
        await lock.release()
```

**`enqueue()`** connects to the server and sends the `e` command. Returns `"acquired"` if the lock was free (fast path) or `"queued"` if there are other holders/waiters. On fast-path acquire, the renewal task starts immediately.

**`wait(timeout_s=None)`** sends the `w` command and blocks until the lock is granted. Returns `True` on success, `False` on timeout. If the lock was already acquired during `enqueue()`, returns `True` immediately without contacting the server. Uses `acquire_timeout_s` if `timeout_s` is not provided.

## Low-level functions

The module also exposes low-level protocol functions for direct use:

```python
from dflockd_client.client import acquire, release, renew, enqueue, wait

reader, writer = await asyncio.open_connection("127.0.0.1", 6388)

token, lease = await acquire(reader, writer, "my-key", 10)
remaining = await renew(reader, writer, "my-key", token)
await release(reader, writer, "my-key", token)

writer.close()
await writer.wait_closed()
```

The two-phase functions are also available at the low level:

```python
reader, writer = await asyncio.open_connection("127.0.0.1", 6388)

status, token, lease = await enqueue(reader, writer, "my-key")
# status is "acquired" or "queued"

if status == "queued":
    token, lease = await wait(reader, writer, "my-key", wait_timeout_s=10)

await release(reader, writer, "my-key", token)

writer.close()
await writer.wait_closed()
```

## Semaphores

`DistributedSemaphore` allows up to N concurrent holders on the same key. It has the same API as `DistributedLock` plus a required `limit` parameter.

### Context manager

```python
from dflockd_client.client import DistributedSemaphore

async with DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10) as sem:
    print(f"token={sem.token} lease={sem.lease}")
    # up to 3 holders at once
```

### Manual acquire/release

```python
sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10)
if await sem.acquire():
    try:
        pass  # critical section
    finally:
        await sem.release()
```

### Two-phase semaphore acquisition

```python
sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10)

status = await sem.enqueue()  # "acquired" or "queued"
await notify_external_system(status)

if await sem.wait(timeout_s=10):
    try:
        pass  # critical section
    finally:
        await sem.release()
```

### Semaphore parameters

`key` is the only positional parameter. All others (including `limit`) are keyword-only.

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
| `auth_token` | `str \| None` | `None` | Auth token for servers started with `--auth-token`. `None` skips auth |
| `connect_timeout_s` | `float` | `10` | Seconds to wait for the TCP connection |

### Semaphore low-level functions

```python
from dflockd_client.client import sem_acquire, sem_release, sem_renew, sem_enqueue, sem_wait

reader, writer = await asyncio.open_connection("127.0.0.1", 6388)

token, lease = await sem_acquire(reader, writer, "my-key", 10, 3)
remaining = await sem_renew(reader, writer, "my-key", token)
await sem_release(reader, writer, "my-key", token)

writer.close()
await writer.wait_closed()
```

## Signals (pub/sub)

`SignalConn` provides pub/sub messaging through named channels with NATS-style wildcard pattern matching.

### Context manager

```python
from dflockd_client.client import SignalConn

async with SignalConn(server=("127.0.0.1", 6388)) as sc:
    await sc.listen("events.>")
    async for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
```

### Listen and emit

```python
sc = SignalConn(server=("127.0.0.1", 6388))
await sc.connect()

await sc.listen("events.user.*")          # subscribe with wildcard
n = await sc.emit("events.user.login", "alice")  # publish; returns delivery count
await sc.unlisten("events.user.*")        # unsubscribe

await sc.aclose()
```

### Wildcard patterns

- `*` matches exactly one dot-separated token: `events.*.login` matches `events.user.login`
- `>` matches one or more trailing tokens: `events.>` matches `events.user.login`, `events.order.created`

### Queue groups

Queue groups provide load-balanced delivery — within a group, each signal is delivered to exactly one member via round-robin:

```python
await sc.listen("jobs.>", group="workers")
```

Multiple queue groups on the same pattern operate independently.

### Consuming signals

Signals are delivered asynchronously via a background reader task. There are two ways to consume them:

**Async iteration** (recommended):

```python
async for sig in sc:
    print(sig.channel, sig.payload)
```

Iteration ends cleanly when the connection is closed.

**Direct queue access:**

```python
sig = await sc.signals.get()
if sig is None:
    print("connection closed")
```

The `signals` property returns an `asyncio.Queue[Signal | None]`. A `None` sentinel indicates the connection has been closed.

### Signal parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `server` | `tuple[str, int]` | `("127.0.0.1", 6388)` | Server address |
| `ssl_context` | `ssl.SSLContext \| None` | `None` | TLS context. `None` uses plain TCP |
| `auth_token` | `str \| None` | `None` | Auth token. `None` skips auth |
| `connect_timeout_s` | `float` | `10` | Seconds to wait for the TCP connection |

### Low-level sig_emit

For fire-and-forget publishing without a `SignalConn` (no background reader needed):

```python
from dflockd_client.client import sig_emit

reader, writer = await asyncio.open_connection("127.0.0.1", 6388)
n = await sig_emit(reader, writer, "events.user.login", "alice")
writer.close()
await writer.wait_closed()
```

## Stats

Query the server for current state using the low-level `stats()` function:

```python
from dflockd_client.client import stats

reader, writer = await asyncio.open_connection("127.0.0.1", 6388)
result = await stats(reader, writer)
print(result)
# {'connections': 1, 'locks': [], 'semaphores': [], 'idle_locks': [], 'idle_semaphores': []}
writer.close()
await writer.wait_closed()
```

Returns a `StatsResult` TypedDict with:

| Field | Type | Description |
|---|---|---|
| `connections` | `int` | Number of connected TCP clients |
| `locks` | `list[dict]` | Held locks with `key`, `owner_conn_id`, `lease_expires_in_s`, `waiters` |
| `semaphores` | `list[dict]` | Active semaphores with `key`, `limit`, `holders`, `waiters` |
| `idle_locks` | `list` | Unused lock entries |
| `idle_semaphores` | `list` | Unused semaphore entries |
