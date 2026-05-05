# Async Client

The async client uses `asyncio` for non-blocking operations. The API is
the same shape as the [Sync Client](sync.md) — every method is just
prefixed with `await` and uses `async with`.

## Imports

```python
from dflockd_client import (
    AsyncDistributedLock,
    AsyncDistributedSemaphore,
    AsyncConn,
)
```

## DistributedLock

```python
import asyncio
from dflockd_client import AsyncDistributedLock

async def main():
    async with AsyncDistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")

asyncio.run(main())
```

If `acquire_timeout_s` elapses without a grant, `__aenter__` raises
`TimeoutError`. The lease auto-renews in an `asyncio.Task` for as long as
the lock is held.

### Manual acquire / release

```python
lock = AsyncDistributedLock("my-key", acquire_timeout_s=10)
if await lock.acquire():
    try:
        ...
    finally:
        await lock.release()
```

### Parameters

Identical to [SyncDistributedLock](sync.md#parameters).

### Methods

| Method | Returns | Notes |
|---|---|---|
| `await acquire()` | `bool` | `False` on server-side timeout |
| `await enqueue()` | `str` | Two-phase step 1: `"acquired"` or `"queued"` |
| `await wait(timeout_s=None)` | `bool` | Two-phase step 2 |
| `await release()` | `bool` | Stop renewal, send release, close connection |
| `await aclose()` | `None` | Stop renewal, close connection (no release sent) |

### Two-phase acquisition

```python
lock = AsyncDistributedLock("my-key", acquire_timeout_s=10)
status = await lock.enqueue()

if status == "queued":
    if await lock.wait(timeout_s=10):
        try:
            ...
        finally:
            await lock.release()
```

`enqueue()` is non-blocking and may fast-path to `"acquired"`, in which
case `wait()` returns `True` without I/O. On timeout, the connection is
closed and the caller must `enqueue()` again to re-queue.

### Background renewal

Once a lock is held, an `asyncio.Task` sends `renew` requests every
`lease * renew_ratio` seconds. Cancellation propagates through `await`
and the task exits cleanly. Renewal failure is logged and the task
exits.

If the instance is garbage-collected while still holding a connection,
`__del__` closes the underlying transport and emits a `ResourceWarning`.

## DistributedSemaphore

Same shape as `DistributedLock` plus a required `limit`:

```python
async with AsyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=10) as sem:
    print(f"token={sem.token}")
```

A `Semaphore` with `limit=1` is exactly equivalent to a `Lock`. Mixing
the two on the same key surfaces `LimitMismatchError`.

## Low-level API

```python
import asyncio
from dflockd_client._async import open_conn, acquire, release, renew

async def main():
    conn = await open_conn(
        "127.0.0.1", 6388, ssl_context=None, connect_timeout_s=5.0
    )
    try:
        token, lease = await acquire(conn, "my-key", acquire_timeout_s=10)
        remaining = await renew(conn, "my-key", token)
        await release(conn, "my-key", token)
    finally:
        await conn.close()

asyncio.run(main())
```

### Functions

```python
async def acquire(conn, key, acquire_timeout_s, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, int]
async def release(conn, key, token, *, prefix="") -> None
async def renew(conn, key, token, lease_ttl_s=None, *, prefix="", read_timeout=30.0) -> int
async def enqueue(conn, key, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, str | None, int | None]
async def wait(conn, key, wait_timeout_s, *, prefix="") -> tuple[str, int]

async def sem_acquire(conn, key, acquire_timeout_s, limit, lease_ttl_s=None) -> tuple[str, int]
async def sem_release(conn, key, token) -> None
async def sem_renew(conn, key, token, lease_ttl_s=None) -> int
async def sem_enqueue(conn, key, limit, lease_ttl_s=None) -> tuple[str, str | None, int | None]
async def sem_wait(conn, key, wait_timeout_s) -> tuple[str, int]

async def stats(conn) -> StatsResult
async def authenticate(conn, auth_token) -> None
async def open_conn(host, port, *, ssl_context, connect_timeout_s) -> AsyncConn
```

The `(prefix, limit)` invariant matches the sync client: `prefix=""`
(lock) rejects `limit`; `prefix="s"` (semaphore) requires it.

If a low-level call raises, treat the connection as broken — close it
and dial a fresh one.

## Sync vs async at a glance

|                       | Sync                          | Async                          |
|-----------------------|-------------------------------|--------------------------------|
| Lock                  | `SyncDistributedLock`         | `AsyncDistributedLock`         |
| Semaphore             | `SyncDistributedSemaphore`    | `AsyncDistributedSemaphore`    |
| Transport             | `SyncConn`                    | `AsyncConn`                    |
| Renewal               | `threading.Thread` (daemon)   | `asyncio.Task`                 |
| Context manager       | `with`                        | `async with`                   |
| Cleanup               | `lock.close()`                | `await lock.aclose()`          |
| Best for              | scripts, threaded servers     | asyncio applications           |
