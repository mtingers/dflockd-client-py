# Sharding

When you point the client at more than one dflockd server, it picks one
per key using a `ShardingStrategy`. The default is CRC-32 — the same
key always lands on the same server, and a heterogeneous fleet of Go,
Python, and TypeScript clients all agree on the routing.

## Default strategy

```python
def stable_hash_shard(key: str, num_servers: int) -> int:
    return zlib.crc32(key.encode("utf-8")) % num_servers
```

`zlib.crc32` is deterministic across processes regardless of
`PYTHONHASHSEED`, unlike Python's built-in `hash()`.

## Multi-server example

```python
from dflockd_client import SyncDistributedLock

servers = [
    ("lock-a", 6388),
    ("lock-b", 6388),
    ("lock-c", 6388),
]

with SyncDistributedLock("user:42:profile", servers=servers) as lock:
    # always lands on the same server for "user:42:profile"
    ...
```

## Custom strategies

Provide any callable with the signature `(key: str, num_servers: int) -> int`:

```python
import zlib
from dflockd_client import SyncDistributedLock

def region_shard(key: str, n: int) -> int:
    """Pin EU-prefixed keys to server 0; hash the rest."""
    if key.startswith("eu-"):
        return 0
    return zlib.crc32(key.encode()) % n

servers = [("eu", 6388), ("us-1", 6388), ("us-2", 6388)]
with SyncDistributedLock("eu-job-1", servers=servers, sharding_strategy=region_shard) as lock:
    ...  # routed to "eu"
```

The function must return an index in `[0, num_servers)`. A custom
strategy that returns out-of-range surfaces as `IndexError` from the
client.

## Type signature

```python
from dflockd_client import ShardingStrategy
# ShardingStrategy = Callable[[str, int], int]
```

## High availability

Each dflockd server is independent — there is no replication or
consensus between servers. If a sharded server goes down, locks for keys
that hash to it become unavailable until it's back. For higher
availability, run dflockd behind a TCP load balancer with health checks,
or use a consensus-based system if strict failover is required.
