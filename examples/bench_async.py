"""
Async benchmark: N concurrent workers each acquire/release a shared lock
repeatedly and report latency statistics.

Usage:
    uv run python examples/bench_async.py [--workers 10] [--rounds 50] [--key bench] \
        [--servers host1:port1,host2:port2]
"""

import argparse
import asyncio
import random
import statistics
import time

from dflockd_client._async import acquire, open_conn, release


def parse_servers(raw: str) -> list[tuple[str, int]]:
    servers = []
    for entry in raw.split(","):
        host, port_str = entry.rsplit(":", 1)
        servers.append((host, int(port_str)))
    return servers


async def worker(
    key: str,
    rounds: int,
    timeout_s: int,
    server: tuple[str, int],
) -> list[float]:
    conn = await open_conn(
        server[0], server[1], ssl_context=None, connect_timeout_s=5.0
    )
    latencies: list[float] = []
    try:
        for _ in range(rounds):
            t0 = time.perf_counter()
            token, _ = await acquire(conn, key, timeout_s, lease_ttl_s=10)
            await release(conn, key, token)
            latencies.append(time.perf_counter() - t0)
    finally:
        await conn.close()
    return latencies


async def run(
    workers: int, rounds: int, key: str, timeout_s: int, servers: list[tuple[str, int]]
) -> None:
    print(f"bench_async: {workers} workers x {rounds} rounds (key_prefix={key!r})")
    print()

    server = servers[0]
    t_start = time.perf_counter()
    results = await asyncio.gather(
        *(
            worker(
                f"{key}_{random.randint(100000, 10000000)}",
                rounds,
                timeout_s,
                server,
            )
            for _ in range(workers)
        )
    )
    wall = time.perf_counter() - t_start

    all_latencies: list[float] = []
    for lats in results:
        all_latencies.extend(lats)

    total_ops = len(all_latencies)
    mean = statistics.mean(all_latencies)
    mn = min(all_latencies)
    mx = max(all_latencies)
    p50 = statistics.median(all_latencies)
    stdev = statistics.stdev(all_latencies) if total_ops > 1 else 0.0

    print(f"  total ops : {total_ops}")
    print(f"  wall time : {wall:.3f}s")
    print(f"  throughput: {total_ops / wall:.1f} ops/s")
    print()
    print(f"  mean      : {mean * 1000:.3f} ms")
    print(f"  min       : {mn * 1000:.3f} ms")
    print(f"  max       : {mx * 1000:.3f} ms")
    print(f"  p50       : {p50 * 1000:.3f} ms")
    print(f"  stdev     : {stdev * 1000:.3f} ms")


def main() -> None:
    parser = argparse.ArgumentParser(description="Async dflockd benchmark")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--rounds", type=int, default=50)
    parser.add_argument("--key", default="bench")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--servers", default="127.0.0.1:6388", help="Comma-separated host:port pairs"
    )
    args = parser.parse_args()
    servers = parse_servers(args.servers)
    asyncio.run(run(args.workers, args.rounds, args.key, args.timeout, servers))


if __name__ == "__main__":
    main()
