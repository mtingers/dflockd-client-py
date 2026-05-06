# Sync Client

The sync client uses the standard library's `socket` and `threading` for
blocking operations. Use it from synchronous scripts, threaded servers,
or any place asyncio isn't appropriate.

## Imports

```python
from dflockd_client import (
    SyncDistributedLock,
    SyncDistributedSemaphore,
    SyncConn,
)
```

## DistributedLock

```python
from dflockd_client import SyncDistributedLock

with SyncDistributedLock("my-key", acquire_timeout_s=10) as lock:
    print(f"token={lock.token} lease={lock.lease}")
```

If `acquire_timeout_s` elapses without a grant, `__enter__` raises
`TimeoutError`. The lease auto-renews in a daemon thread for as long as
the lock is held; the thread is stopped on exit.

### Manual acquire / release

```python
lock = SyncDistributedLock("my-key", acquire_timeout_s=10)
if lock.acquire():
    try:
        ...
    finally:
        lock.release()
```

`acquire()` returns `False` on a server-side timeout, raises any other
error.

### Parameters

`key` is the only positional parameter; everything else is keyword-only.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `key` | `str` | *(required)* | Lock name |
| `acquire_timeout_s` | `int` | `10` | Server-side wait, in seconds. `0` is non-blocking try-lock |
| `lease_ttl_s` | `int \| None` | `None` | Lease duration. `None` uses the server's `--default-lease-ttl` |
| `servers` | `list[tuple[str, int]]` | `[("127.0.0.1", 6388)]` | Server addresses |
| `sharding_strategy` | `ShardingStrategy` | `stable_hash_shard` | `(key, n) → index` |
| `renew_ratio` | `float` | `0.5` | Renew at `lease * ratio` seconds |
| `ssl_context` | `ssl.SSLContext \| None` | `None` | TLS context |
| `auth_token` | `str \| None` | `None` | Shared secret for `--auth-token` servers |
| `connect_timeout_s` | `float` | `10.0` | TCP connect timeout |

### Methods

| Method | Returns | Notes |
|---|---|---|
| `acquire()` | `bool` | `False` on server-side timeout |
| `enqueue()` | `str` | Two-phase step 1: `"acquired"` or `"queued"` |
| `wait(timeout_s=None)` | `bool` | Two-phase step 2; `False` on timeout |
| `release()` | `bool` | Stop renewal, send release, close connection |
| `close()` | `None` | Stop renewal, close connection (no release sent) |

### Attributes

| Attribute | Type | Description |
|---|---|---|
| `token` | `str \| None` | Token issued by the server. `None` if not held |
| `lease` | `int` | Most recent server-reported remaining seconds |

### Two-phase acquisition

```python
lock = SyncDistributedLock("my-key", acquire_timeout_s=10)
status = lock.enqueue()           # "acquired" or "queued"

if status == "acquired":
    ...
else:
    if lock.wait(timeout_s=10):   # blocks server-side up to 10s
        try:
            ...
        finally:
            lock.release()
```

`enqueue()` is non-blocking. The fast-path return is `"acquired"`, in
which case renewal starts immediately and `wait()` is a no-op. Otherwise
the call returns `"queued"` and the same instance must be used for the
follow-up `wait()`.

If `wait()` times out, the connection is closed; you must `enqueue()`
again to re-queue.

### Background renewal

Once a lock is held, a daemon `threading.Thread` sends `renew` requests
every `lease * renew_ratio` seconds. Renewal failure is logged and the
thread exits. The client closes the broken connection, clears `token`
and `lease`, and the server-side lease expires on its own if needed.
Renewal stops on `release()`, `close()`, or context-manager exit.

If the instance is garbage-collected while still holding a connection,
`__del__` closes the underlying socket and emits a `ResourceWarning`.

## DistributedSemaphore

Same shape as `DistributedLock` plus a required `limit`:

```python
from dflockd_client import SyncDistributedSemaphore

with SyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=10) as sem:
    print(f"token={sem.token}")
```

A `Semaphore` with `limit=1` is exactly equivalent to a `Lock`. Mixing
the two on the same key surfaces `LimitMismatchError`.

### Methods

Identical to `DistributedLock` — the difference is purely the
`limit`-aware wire commands (`sl`/`se`/`sw`/`sr`/`sn`).

## Low-level API

For applications that want explicit connection management — e.g. holding
many short-lived locks under a single TCP connection — drop down to
`SyncConn` and the `dflockd_client._sync` module-level functions.

```python
import socket
from dflockd_client import SyncConn
from dflockd_client._sync import acquire, release, renew

sock = socket.create_connection(("127.0.0.1", 6388))
conn = SyncConn(sock)
try:
    token, lease = acquire(conn, "my-key", acquire_timeout_s=10)
    remaining = renew(conn, "my-key", token)
    release(conn, "my-key", token)
finally:
    conn.close()
```

`SyncConn` is safe for concurrent use from multiple threads — an
internal mutex serialises request/response pairs, so concurrent
`acquire(conn, "k1", ...)` and `acquire(conn, "k2", ...)` from
different threads share one TCP socket.

### Functions

```python
# locks
def acquire(conn, key, acquire_timeout_s, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, int]
def release(conn, key, token, *, prefix="") -> None
def renew(conn, key, token, lease_ttl_s=None, *, prefix="", read_timeout=30.0) -> int
def enqueue(conn, key, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, str | None, int | None]
def wait(conn, key, wait_timeout_s, *, prefix="") -> tuple[str, int]

# semaphore convenience wrappers (same with prefix="s" and limit set)
def sem_acquire(conn, key, acquire_timeout_s, limit, lease_ttl_s=None) -> tuple[str, int]
def sem_release(conn, key, token) -> None
def sem_renew(conn, key, token, lease_ttl_s=None) -> int
def sem_enqueue(conn, key, limit, lease_ttl_s=None) -> tuple[str, str | None, int | None]
def sem_wait(conn, key, wait_timeout_s) -> tuple[str, int]

# misc
def stats(conn) -> StatsResult
def authenticate(conn, auth_token) -> None
def open_conn(host, port, *, ssl_context, connect_timeout_s) -> SyncConn
```

`acquire`/`enqueue` enforce a `(prefix, limit)` invariant: `prefix=""`
(lock) rejects `limit`; `prefix="s"` (semaphore) requires it. The
`sem_*` wrappers exist so callers don't have to spell out `prefix="s"`
manually.

If a low-level call raises, treat the connection as broken — close it
and dial a fresh one.

## Async equivalents

The async client provides the same surface with `async`/`await`. See
[Async Client](async.md).
