# Python API

## dflockd_client (top-level exports)

The package provides convenience imports at the top level so you don't need to reach into submodules:

```python
from dflockd_client import (
    AsyncDistributedLock,
    AsyncDistributedSemaphore,
    AsyncSignalConn,
    SyncDistributedLock,
    SyncDistributedSemaphore,
    SyncSignalConn,
    Signal,
    StatsResult,
    __version__,
    DEFAULT_SERVERS,
    ShardingStrategy,
    stable_hash_shard,
)
```

| Name | Type | Description |
|---|---|---|
| `AsyncDistributedLock` | `class` | Alias for `dflockd_client.client.DistributedLock` |
| `AsyncDistributedSemaphore` | `class` | Alias for `dflockd_client.client.DistributedSemaphore` |
| `SyncDistributedLock` | `class` | Alias for `dflockd_client.sync_client.DistributedLock` |
| `SyncDistributedSemaphore` | `class` | Alias for `dflockd_client.sync_client.DistributedSemaphore` |
| `StatsResult` | `TypedDict` | Return type of `stats()` with `connections`, `locks`, `semaphores`, `idle_locks`, `idle_semaphores` |
| `__version__` | `str` | Installed package version (e.g. `"1.8.9"`) |
| `DEFAULT_SERVERS` | `tuple[tuple[str, int], ...]` | Default server list: `(("127.0.0.1", 6388),)` |
| `ShardingStrategy` | `Callable[[str, int], int]` | Type alias for sharding callables |
| `stable_hash_shard` | `function` | Default CRC-32 sharding strategy |
| `Signal` | `NamedTuple` | Received signal with `channel` and `payload` fields |
| `AsyncSignalConn` | `class` | Alias for `dflockd_client.client.SignalConn` |
| `SyncSignalConn` | `class` | Alias for `dflockd_client.sync_client.SignalConn` |

---

## dflockd_client.client (async)

### DistributedLock

```python
@dataclass
class DistributedLock:
    key: str
    _: KW_ONLY
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
```

`key` is the only positional parameter. All others are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `await acquire()` | `bool` | Acquire the lock. Returns `False` on timeout |
| `await enqueue()` | `str` | Two-phase step 1: join queue. Returns `"acquired"` or `"queued"` |
| `await wait(timeout_s=None)` | `bool` | Two-phase step 2: block until granted. Returns `False` on timeout |
| `await release()` | `bool` | Release the lock and stop renewal |
| `await aclose()` | `None` | Close the connection and clean up |

**Context manager:**

```python
async with DistributedLock("key") as lock:
    ...  # lock.token, lock.lease available
```

Raises `TimeoutError` if the lock cannot be acquired.

### Low-level functions

#### acquire

```python
async def acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

Send a lock request. Returns `(token, lease_ttl)`. Raises `TimeoutError` on timeout.

#### release

```python
async def release(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
) -> None
```

Send a release request. Raises `RuntimeError` on failure.

#### renew

```python
async def renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

Send a renew request. Returns seconds remaining, or `-1` if not reported by the server. Raises `RuntimeError` on failure.

#### enqueue

```python
async def enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]
```

Two-phase step 1: join the FIFO queue. Returns `(status, token, lease)` where status is `"acquired"` or `"queued"`. Raises `RuntimeError` on failure.

#### wait

```python
async def wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]
```

Two-phase step 2: block until lock is granted. Returns `(token, lease)`. Raises `TimeoutError` on timeout.

### stats

```python
async def stats(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> StatsResult
```

Query server state. Returns a `StatsResult` TypedDict with `connections` (int), `locks` (list), `semaphores` (list), `idle_locks` (list), and `idle_semaphores` (list). Raises `RuntimeError` on failure.

### DistributedSemaphore

```python
@dataclass
class DistributedSemaphore:
    key: str
    _: KW_ONLY
    limit: int
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
```

`key` is the only positional parameter. All others (including `limit`) are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `await acquire()` | `bool` | Acquire a semaphore slot. Returns `False` on timeout |
| `await enqueue()` | `str` | Two-phase step 1: join queue. Returns `"acquired"` or `"queued"` |
| `await wait(timeout_s=None)` | `bool` | Two-phase step 2: block until granted. Returns `False` on timeout |
| `await release()` | `bool` | Release the slot and stop renewal |
| `await aclose()` | `None` | Close the connection and clean up |

**Context manager:**

```python
async with DistributedSemaphore("key", limit=3) as sem:
    ...  # sem.token, sem.lease available
```

Raises `TimeoutError` if a slot cannot be acquired.

### Semaphore low-level functions

#### sem_acquire

```python
async def sem_acquire(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

Send a semaphore acquire request. Returns `(token, lease_ttl)`. Raises `TimeoutError` on timeout, `RuntimeError` on errors (including `error_limit_mismatch`).

#### sem_release

```python
async def sem_release(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
) -> None
```

#### sem_renew

```python
async def sem_renew(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

#### sem_enqueue

```python
async def sem_enqueue(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]
```

Two-phase step 1: join the semaphore FIFO queue. Returns `(status, token, lease)` where status is `"acquired"` or `"queued"`.

#### sem_wait

```python
async def sem_wait(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]
```

Two-phase step 2: block until a semaphore slot is granted. Returns `(token, lease)`. Raises `TimeoutError` on timeout.

### SignalConn

```python
@dataclass
class SignalConn:
    _: KW_ONLY
    server: tuple[str, int] = ("127.0.0.1", 6388)
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
    heartbeat_interval_s: float = 15.0
```

All parameters are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `await connect()` | `None` | Connect to the server and start background reader and heartbeat |
| `await listen(pattern, *, group="")` | `None` | Subscribe to signals matching pattern |
| `await unlisten(pattern, *, group="")` | `None` | Remove a signal subscription |
| `await emit(channel, payload)` | `int` | Publish a signal. Returns delivery count |
| `await aclose()` | `None` | Close the connection |

**Context manager / iteration:**

```python
async with SignalConn(server=("127.0.0.1", 6388)) as sc:
    await sc.listen("events.>")
    async for sig in sc:  # sig.channel, sig.payload
        ...
```

**Property:**

| Property | Type | Description |
|---|---|---|
| `signals` | `asyncio.Queue[Signal \| None]` | Queue of received signals. `None` sentinel indicates connection closed |

### Low-level signal functions

#### sig_emit

```python
async def sig_emit(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    channel: str,
    payload: str,
) -> int
```

Emit a signal on a literal channel (no wildcards). Returns the number of listeners delivered to. Works on a plain reader/writer pair without a `SignalConn`.

---

## dflockd_client.sync_client

### DistributedLock

```python
@dataclass
class DistributedLock:
    key: str
    _: KW_ONLY
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
```

`key` is the only positional parameter. All others are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `acquire()` | `bool` | Acquire the lock. Returns `False` on timeout |
| `enqueue()` | `str` | Two-phase step 1: join queue. Returns `"acquired"` or `"queued"` |
| `wait(timeout_s=None)` | `bool` | Two-phase step 2: block until granted. Returns `False` on timeout |
| `release()` | `bool` | Release the lock and stop renewal |
| `close()` | `None` | Close the connection and clean up |

**Context manager:**

```python
with DistributedLock("key") as lock:
    ...  # lock.token, lock.lease available
```

### Low-level functions

#### acquire

```python
def acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

#### release

```python
def release(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
) -> None
```

#### renew

```python
def renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

#### enqueue

```python
def enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]
```

Two-phase step 1: join the FIFO queue. Returns `(status, token, lease)` where status is `"acquired"` or `"queued"`.

#### wait

```python
def wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]
```

Two-phase step 2: block until lock is granted. Returns `(token, lease)`. Raises `TimeoutError` on timeout.

### stats

```python
def stats(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
) -> StatsResult
```

Query server state. Returns a `StatsResult` TypedDict with `connections` (int), `locks` (list), `semaphores` (list), `idle_locks` (list), and `idle_semaphores` (list). Raises `RuntimeError` on failure.

### DistributedSemaphore

```python
@dataclass
class DistributedSemaphore:
    key: str
    _: KW_ONLY
    limit: int
    acquire_timeout_s: int = 10
    lease_ttl_s: int | None = None
    servers: list[tuple[str, int]] = [("127.0.0.1", 6388)]
    sharding_strategy: ShardingStrategy = stable_hash_shard
    renew_ratio: float = 0.5
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
```

`key` is the only positional parameter. All others (including `limit`) are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `acquire()` | `bool` | Acquire a semaphore slot. Returns `False` on timeout |
| `enqueue()` | `str` | Two-phase step 1: join queue. Returns `"acquired"` or `"queued"` |
| `wait(timeout_s=None)` | `bool` | Two-phase step 2: block until granted. Returns `False` on timeout |
| `release()` | `bool` | Release the slot and stop renewal |
| `close()` | `None` | Close the connection and clean up |

**Context manager:**

```python
with DistributedSemaphore("key", limit=3) as sem:
    ...  # sem.token, sem.lease available
```

### Semaphore low-level functions

#### sem_acquire

```python
def sem_acquire(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    acquire_timeout_s: int,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, int]
```

#### sem_release

```python
def sem_release(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
) -> None
```

#### sem_renew

```python
def sem_renew(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    token: str,
    lease_ttl_s: int | None = None,
) -> int
```

#### sem_enqueue

```python
def sem_enqueue(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    limit: int,
    lease_ttl_s: int | None = None,
) -> tuple[str, str | None, int | None]
```

Two-phase step 1: join the semaphore FIFO queue. Returns `(status, token, lease)` where status is `"acquired"` or `"queued"`.

#### sem_wait

```python
def sem_wait(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    key: str,
    wait_timeout_s: int,
) -> tuple[str, int]
```

Two-phase step 2: block until a semaphore slot is granted. Returns `(token, lease)`. Raises `TimeoutError` on timeout.

### SignalConn

```python
@dataclass
class SignalConn:
    _: KW_ONLY
    server: tuple[str, int] = ("127.0.0.1", 6388)
    ssl_context: ssl.SSLContext | None = None
    auth_token: str | None = None
    connect_timeout_s: float = 10
    heartbeat_interval_s: float = 15.0
```

All parameters are keyword-only.

**Methods:**

| Method | Returns | Description |
|---|---|---|
| `connect()` | `None` | Connect to the server and start background reader thread and heartbeat |
| `listen(pattern, *, group="")` | `None` | Subscribe to signals matching pattern |
| `unlisten(pattern, *, group="")` | `None` | Remove a signal subscription |
| `emit(channel, payload)` | `int` | Publish a signal. Returns delivery count |
| `close()` | `None` | Close the connection |

**Context manager / iteration:**

```python
with SignalConn(server=("127.0.0.1", 6388)) as sc:
    sc.listen("events.>")
    for sig in sc:  # sig.channel, sig.payload
        ...
```

**Property:**

| Property | Type | Description |
|---|---|---|
| `signals` | `queue.Queue[Signal \| None]` | Queue of received signals. `None` sentinel indicates connection closed |

### Low-level signal functions

#### sig_emit

```python
def sig_emit(
    sock: socket.socket,
    rfile: io.TextIOWrapper,
    channel: str,
    payload: str,
) -> int
```

Emit a signal on a literal channel (no wildcards). Returns the number of listeners delivered to. Works on a plain sock/rfile pair without a `SignalConn`.

---

## dflockd_client.sharding

### ShardingStrategy

```python
ShardingStrategy = Callable[[str, int], int]
```

A callable that maps `(key, num_servers)` to a server index.

### stable_hash_shard

```python
def stable_hash_shard(key: str, num_servers: int) -> int
```

Default sharding strategy using `zlib.crc32`. Deterministic across processes regardless of `PYTHONHASHSEED`.

### DEFAULT_SERVERS

```python
DEFAULT_SERVERS: tuple[tuple[str, int], ...] = (("127.0.0.1", 6388),)
```
