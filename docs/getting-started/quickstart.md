# Quick Start

You need a running [dflockd](https://github.com/mtingers/dflockd) server. By
default, the client connects to `127.0.0.1:6388`.

## Acquire a lock

=== "Sync"

    ```python
    from dflockd_client import SyncDistributedLock

    with SyncDistributedLock("my-key", acquire_timeout_s=10) as lock:
        print(f"token={lock.token} lease={lock.lease}")
        # critical section — lease auto-renews in a daemon thread
    ```

=== "Async"

    ```python
    import asyncio
    from dflockd_client import AsyncDistributedLock

    async def main():
        async with AsyncDistributedLock("my-key", acquire_timeout_s=10) as lock:
            print(f"token={lock.token} lease={lock.lease}")
            # critical section — lease auto-renews in a background task

    asyncio.run(main())
    ```

If the lock cannot be acquired within `acquire_timeout_s`, the context manager
raises `TimeoutError`.

## Manual acquire and release

For cases where a context manager doesn't fit:

=== "Sync"

    ```python
    from dflockd_client import SyncDistributedLock

    lock = SyncDistributedLock("my-key", acquire_timeout_s=10)
    if lock.acquire():
        try:
            ...  # critical section
        finally:
            lock.release()
    ```

=== "Async"

    ```python
    from dflockd_client import AsyncDistributedLock

    lock = AsyncDistributedLock("my-key", acquire_timeout_s=10)
    if await lock.acquire():
        try:
            ...
        finally:
            await lock.release()
    ```

`acquire()` returns `False` on a server-side timeout (no slot opened in
time). Network-level timeouts surface as `TimeoutError`.

## Acquire a semaphore

A semaphore allows up to `limit` concurrent holders on the same key. The
API is identical to `DistributedLock` aside from the required `limit`.

=== "Sync"

    ```python
    from dflockd_client import SyncDistributedSemaphore

    with SyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=10) as sem:
        print(f"token={sem.token}")
    ```

=== "Async"

    ```python
    from dflockd_client import AsyncDistributedSemaphore

    async with AsyncDistributedSemaphore("pool", limit=3, acquire_timeout_s=10) as sem:
        print(f"token={sem.token}")
    ```

A `Semaphore` with `limit=1` is exactly equivalent to a `Lock`. Mixing the
two on the same key surfaces `LimitMismatchError`.

See [Examples](examples.md) for FIFO ordering, two-phase acquisition,
authentication, TLS, and sharding. See
[Architecture](../guide/architecture.md) for how the client works inside.
