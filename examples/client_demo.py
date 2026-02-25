import asyncio
from dataclasses import dataclass

from dflockd_client.client import DistributedLock


async def demo():
    """Acquire a lock, hold it for 45 s (renewing automatically), then release."""
    async with DistributedLock("foo", acquire_timeout_s=10, lease_ttl_s=20) as lock:
        print(f"acquired key={lock.key} token={lock.token} lease={lock.lease}")
        await asyncio.sleep(45)  # will keep renewing
        print("done critical section")

@dataclass
class TaskInfo:
    worker_id: int
    lock: DistributedLock

async def _demo_lock_ordering(task_info: TaskInfo):
    print(f"_demo_lock_ordering[start] enqueued: {task_info.worker_id}")
    if await task_info.lock.wait():
        try:
            print(f"acquired  token({task_info.worker_id}): {task_info.lock.token=} key=foo")
            await asyncio.sleep(1)
        finally:
            await task_info.lock.release()
            # print(f"released  token({task_info.worker_id}): {task_info.lock.token=} key=foo")


async def demo_lock_ordering():
    """Launch several workers that all compete for the same lock (FIFO order)."""
    num_tasks = 9
    tasks = []
    for i in range(num_tasks):
        lock = DistributedLock("foo", acquire_timeout_s=12)
        await lock.enqueue()
        task_info = TaskInfo(worker_id=i, lock=lock)
        tasks.append(_demo_lock_ordering(task_info))
    print(f"launched {num_tasks} workers with shared lock. gathering...")
    await asyncio.gather(*tasks)
    print("all workers finished")


async def demo_multi_server():
    """Demonstrate multi-server sharding: different keys route to different servers."""
    servers = [("127.0.0.1", 6388), ("127.0.0.1", 6389)]
    for key in ("job-a", "job-b", "job-c"):
        async with DistributedLock(key, acquire_timeout_s=10, servers=servers) as lock:
            print(f"key={lock.key} token={lock.token} lease={lock.lease}")


async def demo_timeout():
    """Launch several workers that all compete for the same lock (FIFO order)
    but eventually hit ttl timeout."""
    num_tasks = 2
    tasks = [demo() for _ in range(num_tasks)]
    print(f"launched {num_tasks} workers with shared lock. gathering...")
    await asyncio.gather(*tasks)
    print("all workers finished")


if __name__ == "__main__":
    # asyncio.run(demo())
    asyncio.run(demo_lock_ordering())
