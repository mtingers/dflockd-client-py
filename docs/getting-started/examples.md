# Examples

Every example assumes a running dflockd server on `127.0.0.1:6388`. Adjust
`servers=` for a different address.

## Hold a lock with auto-renewal

The lease renews in the background as long as the lock is held; the lock
is released when the `with` block exits.

=== "Sync"

    ```python
    import time
    from dflockd_client import SyncDistributedLock

    with SyncDistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        time.sleep(45)  # lease renews automatically
    ```

=== "Async"

    ```python
    import asyncio
    from dflockd_client import AsyncDistributedLock

    async def main():
        async with AsyncDistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
            print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
            await asyncio.sleep(45)  # lease renews automatically

    asyncio.run(main())
    ```

## FIFO ordering

Multiple workers competing for the same key are granted in queue order.

=== "Sync"

    ```python
    import threading
    import time
    from dflockd_client import SyncDistributedLock

    def worker(n: int):
        with SyncDistributedLock("foo", acquire_timeout_s=30) as lock:
            print(f"acquired ({n}): {lock.token}")
            time.sleep(0.5)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
    for t in threads: t.start()
    for t in threads: t.join()
    ```

=== "Async"

    ```python
    import asyncio
    from dflockd_client import AsyncDistributedLock

    async def worker(n: int):
        async with AsyncDistributedLock("foo", acquire_timeout_s=30) as lock:
            print(f"acquired ({n}): {lock.token}")
            await asyncio.sleep(0.5)

    async def main():
        await asyncio.gather(*(worker(i) for i in range(9)))

    asyncio.run(main())
    ```

## Two-phase acquisition

Split queue-join from blocking so you can run application logic in
between (e.g. record a telemetry event, notify another system).

=== "Sync"

    ```python
    from dflockd_client import SyncDistributedLock

    lock = SyncDistributedLock("my-key", acquire_timeout_s=10, lease_ttl_s=20)
    status = lock.enqueue()           # "acquired" or "queued"
    notify_external_system(status)    # your logic here

    if lock.wait(timeout_s=10):       # blocks until granted
        try:
            ...
        finally:
            lock.release()
    ```

=== "Async"

    ```python
    from dflockd_client import AsyncDistributedLock

    lock = AsyncDistributedLock("my-key", acquire_timeout_s=10, lease_ttl_s=20)
    status = await lock.enqueue()
    await notify_external_system(status)

    if await lock.wait(timeout_s=10):
        try:
            ...
        finally:
            await lock.release()
    ```

If the lock is free at `enqueue()` time it returns `"acquired"` (fast
path) and the lease starts renewing immediately; `wait()` then returns
`True` without doing any I/O. Otherwise it returns `"queued"` and `wait()`
blocks server-side up to the timeout.

## Bounded concurrency with a semaphore

Up to `limit` holders run concurrently; the rest queue in FIFO order.

=== "Sync"

    ```python
    import threading
    import time
    from dflockd_client import SyncDistributedSemaphore

    def worker(n: int):
        with SyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=30) as sem:
            print(f"acquired ({n}): {sem.token}")
            time.sleep(1)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
    for t in threads: t.start()
    for t in threads: t.join()
    ```

=== "Async"

    ```python
    import asyncio
    from dflockd_client import AsyncDistributedSemaphore

    async def worker(n: int):
        async with AsyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=30) as sem:
            print(f"acquired ({n}): {sem.token}")
            await asyncio.sleep(1)

    async def main():
        await asyncio.gather(*(worker(i) for i in range(9)))

    asyncio.run(main())
    ```

## Handling specific errors

Every server-side error maps to a sentinel exception so callers can
branch with `isinstance` instead of parsing strings.

```python
from dflockd_client import (
    AsyncDistributedLock,
    DflockdTimeoutError,
    MaxLocksError,
    DrainingError,
)

lock = AsyncDistributedLock("my-key")
try:
    if not await lock.acquire():
        # Server-side timeout — no slot opened in acquire_timeout_s
        return
    ...
except MaxLocksError:
    ...  # cluster-wide unique-key cap reached
except DrainingError:
    ...  # server is shutting down
except PermissionError:
    ...  # auth_token rejected
finally:
    await lock.release()
```

`DflockdTimeoutError` is a subclass of `TimeoutError`, so the standard
`except TimeoutError` works too — but `lock.acquire()` returns `False`
on server-side timeout, so most callers don't need to catch it.

## Authentication

Connect to a dflockd server started with `--auth-token`:

=== "Sync"

    ```python
    from dflockd_client import SyncDistributedLock

    with SyncDistributedLock("my-key", auth_token="shared-secret") as lock:
        ...
    ```

=== "Async"

    ```python
    from dflockd_client import AsyncDistributedLock

    async with AsyncDistributedLock("my-key", auth_token="shared-secret") as lock:
        ...
    ```

A wrong or missing token surfaces as `PermissionError`.

## TLS

Pass any `ssl.SSLContext` — the same one you'd use with `socket` or
`asyncio` directly.

=== "Sync"

    ```python
    import ssl
    from dflockd_client import SyncDistributedLock

    ctx = ssl.create_default_context()  # uses system CA bundle
    # or: ssl.create_default_context(cafile="/path/to/ca.pem")

    with SyncDistributedLock("my-key", ssl_context=ctx) as lock:
        ...
    ```

=== "Async"

    ```python
    import ssl
    from dflockd_client import AsyncDistributedLock

    ctx = ssl.create_default_context()

    async with AsyncDistributedLock("my-key", ssl_context=ctx) as lock:
        ...
    ```

## Multi-server sharding

Each key deterministically routes to a single server (CRC-32 of the key).

```python
from dflockd_client import SyncDistributedLock

servers = [("a", 6388), ("b", 6388), ("c", 6388)]

with SyncDistributedLock("user:42:profile", servers=servers) as lock:
    # always lands on the same server for "user:42:profile"
    ...
```

The default strategy matches the Go and TypeScript clients — a
heterogeneous fleet picks the same server for any given key.

## Server stats

```python
from dflockd_client import SyncConn
from dflockd_client._sync import stats
import socket

sock = socket.create_connection(("127.0.0.1", 6388))
conn = SyncConn(sock)
try:
    result = stats(conn)
    print(result["connections"])
    print(result["locks"])
finally:
    conn.close()
```

`result` is a [`StatsResult`](../api/python.md#statsresult) TypedDict with
`connections`, `locks`, `semaphores`, `idle_locks`, and `idle_semaphores`.
