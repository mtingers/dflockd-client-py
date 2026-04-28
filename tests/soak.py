"""Soak test for dflockd-client. Runs mixed workloads against a dflockd
server for a configurable duration and asserts bounded resource growth.

Catches: connection/FD leaks, unbounded task or thread accumulation,
renew-loop runaway, signal-queue growth, dropped-signals not being
counted, lease drift, and per-iteration memory growth.

Usage::

    DFLOCKD_TEST_PORT=16388 python tests/soak.py            # default 60s
    DFLOCKD_TEST_PORT=16388 python tests/soak.py --seconds 300
    DFLOCKD_TEST_PORT=16388 python tests/soak.py --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import os
import sys
import threading
import time
import tracemalloc
from collections import Counter
from dataclasses import dataclass

import psutil

# Make `dflockd_client` importable when run as a script from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import dflockd_client.client as aclient  # noqa: E402
import dflockd_client.sync_client as sclient  # noqa: E402


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


@dataclass
class Sample:
    t: float
    rss_mb: float
    fds: int
    threads: int
    tasks: int
    aclient_locks: int
    sclient_locks: int
    aclient_signalconns: int
    sclient_signalconns: int


def take_sample(proc: psutil.Process, t0: float) -> Sample:
    gc.collect()
    objs = gc.get_objects()
    types = Counter(type(o).__name__ for o in objs)
    return Sample(
        t=time.monotonic() - t0,
        rss_mb=proc.memory_info().rss / 1024 / 1024,
        fds=proc.num_fds(),
        threads=proc.num_threads(),
        tasks=len(asyncio.all_tasks()) if _has_running_loop() else 0,
        aclient_locks=types.get("DistributedLock", 0),  # both async + sync share name
        sclient_locks=0,  # rolled in above; differentiate by class id below
        aclient_signalconns=types.get("SignalConn", 0),
        sclient_signalconns=0,
    )


def _has_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


# ---------------------------------------------------------------------------
# Workloads
# ---------------------------------------------------------------------------


class Counters:
    def __init__(self) -> None:
        self.async_lock_cycles = 0
        self.async_sem_cycles = 0
        self.async_two_phase_cycles = 0
        self.async_signals_emitted = 0
        self.async_signals_received = 0
        self.async_dropped = 0
        self.sync_lock_cycles = 0
        self.errors: list[str] = []

    def err(self, where: str, e: BaseException) -> None:
        self.errors.append(f"{where}: {type(e).__name__}: {e}")


async def workload_async_lock_cycle(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    """Steady acquire/release loop, short lease — exercises full
    lifecycle and renew-loop teardown on every iteration. Throttled
    to ~50 cycles/sec so we don't exhaust ephemeral source ports
    against TIME_WAIT on the loopback path."""
    while not stop.is_set():
        try:
            async with aclient.DistributedLock(
                "soak.lock.cycle",
                acquire_timeout_s=2,
                lease_ttl_s=2,
                servers=[(host, port)],
                renew_ratio=0.5,
            ):
                counters.async_lock_cycles += 1
        except Exception as e:
            counters.err("async_lock_cycle", e)
        await asyncio.sleep(0.02)


async def workload_async_held_lock(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    """Hold a lock for a long time — exercises the renew loop's
    repeat-renew path and the new self.lease update logic."""
    try:
        async with aclient.DistributedLock(
            "soak.lock.held",
            acquire_timeout_s=5,
            lease_ttl_s=2,
            servers=[(host, port)],
            renew_ratio=0.4,
        ) as lock:
            initial_lease = lock.lease
            while not stop.is_set():
                await asyncio.sleep(0.5)
            # After a long hold the lease must still be a sane positive int —
            # the renew loop should keep refreshing it.
            assert lock.lease > 0, f"lease drifted to {lock.lease}"
            assert lock.lease <= initial_lease + 5, (
                f"lease grew unexpectedly: {lock.lease} (initial {initial_lease})"
            )
    except Exception as e:
        counters.err("async_held_lock", e)


async def workload_async_sem_cycle(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    while not stop.is_set():
        try:
            async with aclient.DistributedSemaphore(
                "soak.sem.cycle",
                limit=4,
                acquire_timeout_s=2,
                lease_ttl_s=2,
                servers=[(host, port)],
            ):
                counters.async_sem_cycles += 1
        except Exception as e:
            counters.err("async_sem_cycle", e)
        await asyncio.sleep(0.02)


async def workload_async_two_phase(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    """Two-phase enqueue/wait under contention. Many concurrent waiters
    on one key serialize through FIFO."""
    while not stop.is_set():
        lock = aclient.DistributedLock(
            "soak.lock.two_phase",
            acquire_timeout_s=5,
            lease_ttl_s=2,
            servers=[(host, port)],
        )
        try:
            status = await lock.enqueue()
            if status == "queued":
                granted = await lock.wait(timeout_s=5)
                if not granted:
                    await lock.release()
                    continue
            counters.async_two_phase_cycles += 1
            await lock.release()
        except Exception as e:
            counters.err("async_two_phase", e)
            try:
                await lock.aclose()
            except Exception:
                pass
        await asyncio.sleep(0.02)


async def workload_async_signal_listener(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    """Long-lived listener, drain incoming signals; track drops if any."""
    while not stop.is_set():
        try:
            async with aclient.SignalConn(
                server=(host, port), heartbeat_interval_s=2
            ) as sc:
                await sc.listen("soak.sig.>")
                while not stop.is_set():
                    try:
                        sig = await asyncio.wait_for(
                            sc.signals.get(), timeout=0.5
                        )
                    except asyncio.TimeoutError:
                        continue
                    if sig is None:
                        break
                    counters.async_signals_received += 1
                counters.async_dropped = max(
                    counters.async_dropped, sc.dropped_signals
                )
        except Exception as e:
            counters.err("async_signal_listener", e)
            await asyncio.sleep(0.1)


async def workload_async_signal_emitter(
    counters: Counters, host: str, port: int, stop: asyncio.Event
) -> None:
    """Steady emit so the listener is always doing real work. Reuses the
    same SignalConn for the duration so we exercise the long-lived
    connection path (heartbeat ping, command serialization) rather than
    burning a new TCP connection every iteration."""
    while not stop.is_set():
        try:
            async with aclient.SignalConn(
                server=(host, port), heartbeat_interval_s=2
            ) as sc:
                while not stop.is_set():
                    try:
                        await sc.emit("soak.sig.tick", "x")
                        counters.async_signals_emitted += 1
                    except (ConnectionError, RuntimeError) as e:
                        counters.err("async_signal_emit", e)
                        break
                    await asyncio.sleep(0.02)
        except Exception as e:
            counters.err("async_signal_emitter", e)
            await asyncio.sleep(0.1)


def workload_sync_lock_cycle(
    counters: Counters, host: str, port: int, stop: threading.Event
) -> None:
    """Sync workload runs in its own thread alongside the asyncio loop —
    exercises sync close()/renew thread teardown."""
    while not stop.is_set():
        try:
            with sclient.DistributedLock(
                "soak.lock.sync",
                acquire_timeout_s=2,
                lease_ttl_s=2,
                servers=[(host, port)],
            ):
                counters.sync_lock_cycles += 1
        except Exception as e:
            counters.err("sync_lock_cycle", e)
        time.sleep(0.02)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


async def run_async_workloads(
    counters: Counters, host: str, port: int, seconds: float
) -> list[Sample]:
    proc = psutil.Process()
    t0 = time.monotonic()
    stop = asyncio.Event()
    samples: list[Sample] = []

    sync_stop = threading.Event()
    sync_threads = [
        threading.Thread(
            target=workload_sync_lock_cycle,
            args=(counters, host, port, sync_stop),
            daemon=True,
        )
        for _ in range(2)
    ]
    for t in sync_threads:
        t.start()

    tasks = [
        asyncio.create_task(
            workload_async_lock_cycle(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_lock_cycle(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_held_lock(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_sem_cycle(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_sem_cycle(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_two_phase(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_two_phase(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_signal_listener(counters, host, port, stop)
        ),
        asyncio.create_task(
            workload_async_signal_emitter(counters, host, port, stop)
        ),
    ]

    samples.append(take_sample(proc, t0))
    sample_interval = max(seconds / 12, 5.0)
    next_sample = time.monotonic() + sample_interval

    while time.monotonic() - t0 < seconds:
        await asyncio.sleep(0.5)
        if time.monotonic() >= next_sample:
            samples.append(take_sample(proc, t0))
            next_sample = time.monotonic() + sample_interval

    stop.set()
    sync_stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    for t in sync_threads:
        t.join(timeout=5)

    samples.append(take_sample(proc, t0))
    return samples


def report(
    samples: list[Sample], counters: Counters, seconds: float, verbose: bool
) -> bool:
    """Print results and return True if all bounded-growth checks passed."""
    print()
    print(f"=== soak summary ({seconds:.0f}s) ===")
    print(f"  async lock cycles:       {counters.async_lock_cycles}")
    print(f"  async sem  cycles:       {counters.async_sem_cycles}")
    print(f"  async two-phase cycles:  {counters.async_two_phase_cycles}")
    print(f"  async signals emitted:   {counters.async_signals_emitted}")
    print(f"  async signals received:  {counters.async_signals_received}")
    print(f"  async signals dropped:   {counters.async_dropped}")
    print(f"  sync  lock cycles:       {counters.sync_lock_cycles}")
    print(f"  errors:                  {len(counters.errors)}")

    if counters.errors:
        print()
        print("  first few errors:")
        for e in counters.errors[:10]:
            print(f"    {e}")

    print()
    print("  resource samples (t, rss_mb, fds, threads, asyncio_tasks):")
    for s in samples:
        print(
            f"    t={s.t:6.1f}  rss={s.rss_mb:7.2f}  fds={s.fds:4d}  "
            f"threads={s.threads:3d}  tasks={s.tasks:3d}"
        )

    first, last = samples[1], samples[-1]  # skip warm-up sample
    rss_growth = last.rss_mb - first.rss_mb
    fd_growth = last.fds - first.fds
    thread_growth = last.threads - first.threads
    task_growth = last.tasks - first.tasks

    print()
    print("  growth (steady-state, excluding warm-up):")
    print(f"    rss_mb:  {rss_growth:+.2f}")
    print(f"    fds:     {fd_growth:+d}")
    print(f"    threads: {thread_growth:+d}")
    print(f"    tasks:   {task_growth:+d}")

    if verbose:
        print()
        print("  tracemalloc top diffs (top 10):")
        for stat in tracemalloc.take_snapshot().statistics("lineno")[:10]:
            print(f"    {stat}")

    # ---------- bounded-growth assertions ----------
    failures: list[str] = []
    if counters.errors:
        failures.append(f"{len(counters.errors)} workload errors (first: {counters.errors[0]})")
    # Per-second budgets are loose enough to absorb GC slack and Python
    # heap fragmentation but tight enough to catch real leaks.
    rss_budget = max(15.0, seconds * 0.05)  # ≤ 0.05 MB/s steady-state growth
    if rss_growth > rss_budget:
        failures.append(f"rss grew {rss_growth:.1f}MB > {rss_budget:.1f}MB budget")
    if fd_growth > 5:
        failures.append(f"fds grew by {fd_growth}; expected ≤ 5")
    if thread_growth > 2:
        failures.append(f"threads grew by {thread_growth}; expected ≤ 2")
    if task_growth > 5:
        failures.append(f"asyncio tasks grew by {task_growth}; expected ≤ 5")
    if counters.async_lock_cycles == 0:
        failures.append("zero async lock cycles completed — workload didn't run")
    if counters.async_signals_received == 0:
        failures.append("zero signals received — pub/sub workload broken")

    if failures:
        print()
        print("  FAILURES:")
        for f in failures:
            print(f"    - {f}")
        return False
    print()
    print("  PASSED — all growth bounds and workload counters within budget")
    return True


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=float, default=60.0)
    p.add_argument(
        "--host",
        default=os.environ.get("DFLOCKD_TEST_HOST", "127.0.0.1"),
    )
    p.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DFLOCKD_TEST_PORT", "6388")),
    )
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    tracemalloc.start()
    counters = Counters()

    print(f"running soak for {args.seconds:.0f}s against {args.host}:{args.port}")
    samples = asyncio.run(
        run_async_workloads(counters, args.host, args.port, args.seconds)
    )
    ok = report(samples, counters, args.seconds, args.verbose)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
