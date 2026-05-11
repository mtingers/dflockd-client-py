# Python API

Every public symbol is re-exported from the top-level `dflockd_client`
package. Submodule imports are listed below for completeness, but the
top-level form is the recommended one.

## Top-level package

```python
from dflockd_client import (
    # high-level
    SyncDistributedLock, AsyncDistributedLock,
    SyncDistributedSemaphore, AsyncDistributedSemaphore,

    # transport
    SyncConn, AsyncConn,

    # types & helpers
    StatsResult, ShardingStrategy,
    DEFAULT_SERVERS, stable_hash_shard, fence_from_token,

    # exceptions
    DflockdError, DflockdTimeoutError,
    AuthError, MaxLocksError, MaxWaitersError, LimitMismatchError,
    NotQueuedError, AlreadyQueuedError, LeaseExpiredError, DrainingError,

    # version
    __version__,
)
```

## High-level types

### `SyncDistributedLock`, `AsyncDistributedLock`

Dataclasses. `key` is positional; everything else is keyword-only.

```python
@dataclass
class SyncDistributedLock:
    key: str
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10.0
```

`AsyncDistributedLock` has the same fields. Methods are documented under
[Sync Client](../client/sync.md) and [Async Client](../client/async.md).

### `SyncDistributedSemaphore`, `AsyncDistributedSemaphore`

Same shape as the lock dataclass plus a required keyword-only
`limit: int`. Mixing a `Lock` and `Semaphore` on the same key surfaces
`LimitMismatchError`.

## Transport

### `SyncConn`

```python
class SyncConn:
    def __init__(self, sock: socket.socket) -> None
    def command(self, cmd: str, key: str, arg: str, *, read_timeout: float | None) -> str
    def close(self) -> None
    def shutdown_read(self) -> None
```

Wraps a single TCP/TLS socket. `command()` writes a 3-line frame and
reads exactly one response line. `read_timeout` is enforced via
`socket.settimeout`; pass `None` to block indefinitely.

`SyncConn` is safe for concurrent use from multiple threads — an
internal mutex serialises request/response pairs so a fleet of threads
can hold many keys under a single connection.

### `AsyncConn`

```python
class AsyncConn:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None
    async def command(self, cmd: str, key: str, arg: str, *, read_timeout: float) -> str
    async def close(self) -> None
```

Async equivalent. `read_timeout` is enforced via `asyncio.wait_for`.

## Errors

All sentinel exceptions live in `dflockd_client.errors` and are
re-exported at the top level.

```python
class DflockdError(RuntimeError): ...
class DflockdTimeoutError(TimeoutError): ...   # blocking acquire/wait timed out

class AuthError(DflockdError): ...             # auth rejected mid-session
class MaxLocksError(DflockdError): ...         # cluster-wide unique-key cap
class MaxWaitersError(DflockdError): ...       # per-key waiter cap
class LimitMismatchError(DflockdError): ...    # semaphore key reused with different limit
class NotQueuedError(DflockdError): ...        # wait() without enqueue()
class AlreadyQueuedError(DflockdError): ...    # enqueue() already in flight
class LeaseExpiredError(DflockdError): ...     # promoted slot's lease expired before observation
class DrainingError(DflockdError): ...         # server is shutting down
```

The auth handshake is special: a wrong token surfaces as `PermissionError`,
not `AuthError`, so callers can branch without importing dflockd-specific
types.

## `StatsResult`

```python
class StatsResult(TypedDict):
    connections: int
    locks: list[dict[str, Any]]              # {key, owner_conn_id, lease_expires_in_s, waiters}
    semaphores: list[dict[str, Any]]         # {key, limit, holders, waiters}
    idle_locks: list[dict[str, Any]]         # {key, idle_s}
    idle_semaphores: list[dict[str, Any]]    # {key, idle_s}
```

The pub/sub `signal_channels` field present in pre-v2 servers is gone.

## Sharding

```python
ShardingStrategy = Callable[[str, int], int]
DEFAULT_SERVERS: tuple[tuple[str, int], ...] = (("127.0.0.1", 6388),)

def stable_hash_shard(key: str, num_servers: int) -> int: ...
```

See [Sharding](../guide/sharding.md).

## Fencing tokens

```python
def fence_from_token(token: str) -> int: ...
```

Every grant returns a 32-char hex token whose first 16 hex chars are a
big-endian `uint64` — the **fence prefix** — minted from a server-side
counter that strictly increases on every grant from a dflockd instance
(also across restarts on a non-regressing wall clock, or unconditionally
when the server runs with `--fence-state-file`). `fence_from_token` parses
that prefix, so a grant token doubles as a
[fencing token](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html):
a downstream resource stores the highest fence it has observed for a key
and rejects any write whose fence compares less.

```python
from dflockd_client import SyncDistributedLock, fence_from_token

with SyncDistributedLock("row:42") as lock:
    fence = fence_from_token(lock.token)   # int in [0, 2**64) — pass to your DB / store
    write_row(42, data, fence=fence)
```

Comparison is meaningful **per key** only — the counter is global, so the
prefix order across different keys just reflects when grants happened. A
`limit > 1` semaphore issues a distinct fence per grant, so fencing orders
the grants, not the resource. Raises `ValueError` if the argument is not
exactly 32 hexadecimal characters.

## Submodules

The high-level types and `Conn` classes live in `dflockd_client._sync`
and `dflockd_client._async`. Both submodules expose stable module-level
functions for low-level work:

```python
# dflockd_client._sync
def acquire(conn, key, acquire_timeout_s, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, int]
def release(conn, key, token, *, prefix="") -> None
def renew(conn, key, token, lease_ttl_s=None, *, prefix="", read_timeout=30.0) -> int
def enqueue(conn, key, lease_ttl_s=None, *, prefix="", limit=None) -> tuple[str, str | None, int | None]
def wait(conn, key, wait_timeout_s, *, prefix="") -> tuple[str, int]

def sem_acquire(conn, key, acquire_timeout_s, limit, lease_ttl_s=None) -> tuple[str, int]
def sem_release(conn, key, token) -> None
def sem_renew(conn, key, token, lease_ttl_s=None) -> int
def sem_enqueue(conn, key, limit, lease_ttl_s=None) -> tuple[str, str | None, int | None]
def sem_wait(conn, key, wait_timeout_s) -> tuple[str, int]

def stats(conn) -> StatsResult
def authenticate(conn, auth_token) -> None
def open_conn(host, port, *, ssl_context, connect_timeout_s) -> SyncConn
```

`dflockd_client._async` provides identical signatures with `async def`.
The leading underscore signals these are lower-level; the high-level
types in the top-level package are the recommended entry point.

## Versioning

`dflockd_client.__version__` is the installed package version, read from
the wheel metadata. The wire protocol matches dflockd v2.x exactly; the
client's major version tracks dflockd's major version.
