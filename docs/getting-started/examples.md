# Examples

## Hold a lock with auto-renewal

Acquire a lock, hold it for an extended period while the client automatically renews the lease in the background:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import DistributedLock

    async def main():
        async with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
            print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
            await asyncio.sleep(45)  # lease renews automatically
            print("done critical section")

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import time
    from dflockd_client.sync_client import DistributedLock

    with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        time.sleep(45)  # lease renews automatically
        print("done critical section")
    ```

## FIFO lock ordering

Multiple workers competing for the same lock are granted access in FIFO order:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import DistributedLock

    async def worker(worker_id: int):
        async with DistributedLock("foo", acquire_timeout_s=12) as lock:
            print(f"acquired  ({worker_id}): {lock.token}")
            await asyncio.sleep(1)
            print(f"released  ({worker_id}): {lock.token}")

    async def main():
        tasks = [worker(i) for i in range(9)]
        await asyncio.gather(*tasks)

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import threading
    import time
    from dflockd_client.sync_client import DistributedLock

    def worker(worker_id: int):
        with DistributedLock("foo", acquire_timeout_s=30) as lock:
            print(f"acquired  ({worker_id}): {lock.token}")
            time.sleep(1)
            print(f"released  ({worker_id}): {lock.token}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ```

## Two-phase lock acquisition

Split enqueue and wait to notify an external system between joining the queue and blocking:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import DistributedLock

    async def main():
        lock = DistributedLock("my-key", acquire_timeout_s=10, lease_ttl_s=20)

        status = await lock.enqueue()       # "acquired" or "queued"
        print(f"enqueue: {status}")

        await notify_external_system()      # your application logic

        if await lock.wait(timeout_s=10):   # blocks until granted
            try:
                print(f"lock held: {lock.token}")
                await asyncio.sleep(1)
            finally:
                await lock.release()

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    from dflockd_client.sync_client import DistributedLock

    lock = DistributedLock("my-key", acquire_timeout_s=10, lease_ttl_s=20)

    status = lock.enqueue()           # "acquired" or "queued"
    print(f"enqueue: {status}")

    notify_external_system()          # your application logic

    if lock.wait(timeout_s=10):       # blocks until granted
        try:
            print(f"lock held: {lock.token}")
        finally:
            lock.release()
    ```

If the lock is free at enqueue time, it is acquired immediately (fast path) and `wait()` returns `True` without blocking. The lease auto-renews in the background from the moment of acquisition.

## Semaphore — bounded concurrency

Use `DistributedSemaphore` to allow up to N concurrent holders on the same key:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import DistributedSemaphore

    async def worker(worker_id: int):
        async with DistributedSemaphore("pool", limit=3, acquire_timeout_s=30) as sem:
            print(f"acquired  ({worker_id}): {sem.token}")
            await asyncio.sleep(1)
            print(f"released  ({worker_id}): {sem.token}")

    async def main():
        tasks = [worker(i) for i in range(9)]
        await asyncio.gather(*tasks)

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import threading
    import time
    from dflockd_client.sync_client import DistributedSemaphore

    def worker(worker_id: int):
        with DistributedSemaphore("pool", limit=3, acquire_timeout_s=30) as sem:
            print(f"acquired  ({worker_id}): {sem.token}")
            time.sleep(1)
            print(f"released  ({worker_id}): {sem.token}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(9)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ```

Up to 3 workers run concurrently; the remaining workers wait in FIFO order for a slot to open.

## Two-phase semaphore acquisition

Split enqueue and wait for semaphores, just like locks:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import DistributedSemaphore

    async def main():
        sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10, lease_ttl_s=20)

        status = await sem.enqueue()       # "acquired" or "queued"
        print(f"enqueue: {status}")

        await notify_external_system()     # your application logic

        if await sem.wait(timeout_s=10):   # blocks until granted
            try:
                print(f"semaphore held: {sem.token}")
                await asyncio.sleep(1)
            finally:
                await sem.release()

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    from dflockd_client.sync_client import DistributedSemaphore

    sem = DistributedSemaphore("my-key", limit=3, acquire_timeout_s=10, lease_ttl_s=20)

    status = sem.enqueue()           # "acquired" or "queued"
    print(f"enqueue: {status}")

    notify_external_system()         # your application logic

    if sem.wait(timeout_s=10):       # blocks until granted
        try:
            print(f"semaphore held: {sem.token}")
        finally:
            sem.release()
    ```

## Server stats

Query the server for its current state — connections, held locks, and active semaphores:

=== "Async"

    ```python
    import asyncio
    from dflockd_client.client import stats

    async def main():
        reader, writer = await asyncio.open_connection("127.0.0.1", 6388)
        result = await stats(reader, writer)
        print(f"connections: {result['connections']}")
        print(f"locks: {result['locks']}")
        print(f"semaphores: {result['semaphores']}")
        writer.close()
        await writer.wait_closed()

    asyncio.run(main())
    ```

=== "Sync"

    ```python
    import socket
    from dflockd_client.sync_client import stats

    sock = socket.create_connection(("127.0.0.1", 6388))
    rfile = sock.makefile("r", encoding="utf-8")
    result = stats(sock, rfile)
    print(f"connections: {result['connections']}")
    print(f"locks: {result['locks']}")
    print(f"semaphores: {result['semaphores']}")
    rfile.close()
    sock.close()
    ```

## Multi-server sharding

Distribute keys across multiple dflockd instances. Each key deterministically routes to the same server:

```python
from dflockd_client.sync_client import DistributedLock

servers = [("server1", 6388), ("server2", 6388), ("server3", 6388)]

with DistributedLock("my-key", servers=servers) as lock:
    print(f"token={lock.token} lease={lock.lease}")
```

## Custom sharding strategy

Override the default CRC-32 sharding with your own logic:

```python
from dflockd_client.sync_client import DistributedLock

def my_strategy(key: str, num_servers: int) -> int:
    """Route all keys to the first server."""
    return 0

servers = [("server1", 6388), ("server2", 6388)]

with DistributedLock("my-key", servers=servers, sharding_strategy=my_strategy) as lock:
    print(f"token={lock.token}")
```
