# Sync Client

The sync client uses standard `socket` and `threading` for blocking lock, semaphore, and signal operations with automatic background lease renewal. No asyncio required.

```python
from dflockd_client.sync_client import DistributedLock, DistributedSemaphore, SignalConn
```

**Alternative top-level imports** (equivalent):

```python
from dflockd_client import SyncDistributedLock, SyncDistributedSemaphore, SyncSignalConn
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

## Authentication and TLS

```python
import ssl
from dflockd_client.sync_client import DistributedLock

ctx = ssl.create_default_context()

with DistributedLock("my-key", auth_token="mysecret", ssl_context=ctx) as lock:
    print(f"token={lock.token}")
```

Both parameters also work on `DistributedSemaphore`. A `PermissionError` is raised if the auth token is invalid.

## Attributes

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | The lock token (UUID hex). `None` if not held |
| `lease` | `int` | Lease duration in seconds as reported by the server |

## Background renewal

Once acquired, a daemon thread sends renew requests at `lease * renew_ratio` intervals. If renewal fails, the client logs an error and the renewal loop exits. The lease will eventually expire server-side.

The renewal thread includes staleness checks — if the connection is replaced (e.g. after a reconnect), the old renewal thread detects the identity mismatch and exits cleanly. If the server returns a zero-length lease, the renewal loop falls back to a 30-second interval instead of spinning aggressively.

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

If a client is garbage collected without being properly closed, `__del__` will close the underlying socket and emit a `ResourceWarning` to help catch leaked connections during development.

## Two-phase acquisition

Split enqueue and wait to run application logic between joining the queue and blocking:

```python
lock = DistributedLock("my-key", acquire_timeout_s=10)

status = lock.enqueue()              # "acquired" or "queued"
notify_external_system(status)        # your logic here

if lock.wait(timeout_s=10):           # blocks until granted
    try:
        pass  # critical section
    finally:
        lock.release()
```

- **`enqueue()`** — returns `"acquired"` (fast path, renewal starts immediately) or `"queued"`.
- **`wait(timeout_s=None)`** — blocks until granted. Returns `False` on timeout. No-op if already acquired during `enqueue()`. Defaults to `acquire_timeout_s`.

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

Same as [DistributedLock parameters](#parameters) plus a required `limit: int` (maximum concurrent holders). `key` is positional; all others (including `limit`) are keyword-only.

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

## Signals (pub/sub)

`SignalConn` provides pub/sub messaging through named channels with NATS-style wildcard pattern matching.

### Context manager

```python
from dflockd_client.sync_client import SignalConn

with SignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    for sig in sc:
        print(f"{sig.channel}: {sig.payload}")
```

### Listen and emit

```python
sc = SignalConn(server=("127.0.0.1", 6388))
sc.connect()

sc.listen("events.user.*")          # subscribe with wildcard
n = sc.emit("events.user.login", "alice")  # publish; returns delivery count
sc.unlisten("events.user.*")        # unsubscribe

sc.close()
```

### Wildcard patterns

- `*` matches exactly one dot-separated token: `events.*.login` matches `events.user.login`
- `>` matches one or more trailing tokens: `events.>` matches `events.user.login`, `events.order.created`

### Queue groups

Queue groups provide load-balanced delivery — within a group, each signal is delivered to exactly one member via round-robin:

```python
sc.listen("jobs.>", group="workers")
```

Multiple queue groups on the same pattern operate independently.

### Consuming signals

Signals are delivered asynchronously via a background reader thread. There are two ways to consume them:

**Iteration** (recommended):

```python
for sig in sc:
    print(sig.channel, sig.payload)
```

Iteration ends cleanly when the connection is closed.

**Direct queue access:**

```python
sig = sc.signals.get()
if sig is None:
    print("connection closed")
```

The `signals` property returns a `queue.Queue[Signal | None]`. A `None` sentinel indicates the connection has been closed.

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
import socket
from dflockd_client.sync_client import sig_emit

sock = socket.create_connection(("127.0.0.1", 6388))
rfile = sock.makefile("r", encoding="utf-8")
n = sig_emit(sock, rfile, "events.user.login", "alice")
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

Returns a `StatsResult` TypedDict with:

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
| Top-level alias | `from dflockd_client import AsyncDistributedLock` | `from dflockd_client import SyncDistributedLock` |
| Context manager | `async with` | `with` |
| Renewal | `asyncio.Task` | `threading.Thread` (daemon) |
| Cleanup | `await lock.aclose()` | `lock.close()` |
| Best for | asyncio applications, high concurrency | Scripts, threads, simple applications |
| Signal conn | `SignalConn` / `AsyncSignalConn` | `SignalConn` / `SyncSignalConn` |
| Signal cleanup | `await sc.aclose()` | `sc.close()` |
