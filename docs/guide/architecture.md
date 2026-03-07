# Architecture

## Overview

The dflockd-client library provides async and sync Python clients for [dflockd](https://github.com/mtingers/dflockd), a distributed lock and semaphore server. Both clients communicate with the server over TCP using a line-based UTF-8 protocol.

```
┌─────────────────────────────────┐
│        Your application         │
│                                 │
│  ┌───────────┐  ┌───────────┐  │
│  │  Async    │  │  Sync     │  │
│  │  Client   │  │  Client   │  │
│  │  +Signals │  │  +Signals │  │
│  └─────┬─────┘  └─────┬─────┘  │
│        │              │         │
│  ┌─────┴──────────────┴─────┐  │
│  │     Sharding layer       │  │
│  └─────┬──────────────┬─────┘  │
└────────┼──────────────┼────────┘
         │   TCP        │   TCP
    ┌────▼────┐    ┌────▼────┐
    │ dflockd │    │ dflockd │
    │ server  │    │ server  │
    └─────────┘    └─────────┘
```

## Client lifecycle

### Connection

Both `DistributedLock` and `DistributedSemaphore` extend shared base classes (`_AsyncBase` / `_SyncBase`) that handle connection management, renewal, and cleanup.

When `acquire()` or `enqueue()` is called, the client:

1. Selects a server using the sharding strategy (based on the key).
2. Opens a TCP connection to that server.
3. Sends the lock or semaphore request over the wire.

### Lock acquisition

- **Single-phase (`acquire`)** — sends a lock request with a timeout. The server grants the lock immediately if free, or enqueues the client in FIFO order. The call blocks until the lock is granted or the timeout expires.
- **Two-phase (`enqueue` + `wait`)** — splits acquisition into two steps. `enqueue()` joins the queue and returns immediately with `"acquired"` or `"queued"`. `wait()` blocks until the lock is granted. This allows application logic (e.g. notifying an external system) between joining the queue and blocking.

### Semaphore acquisition

Semaphores follow the same lifecycle as locks but allow up to N concurrent holders per key (controlled by the `limit` parameter). The protocol uses `sl`/`se`/`sw`/`sr`/`sn` commands instead of `l`/`e`/`w`/`r`/`n`. Both `DistributedLock` and `DistributedSemaphore` support single-phase and two-phase acquisition.

### Background renewal

Once a lock or semaphore is acquired, the client starts a background renewal loop:

- **Async client** — an `asyncio.Task` that sends renew requests at `lease * renew_ratio` intervals.
- **Sync client** — a daemon `threading.Thread` that does the same.

If renewal fails (server unreachable, lease already expired), the client logs an error and the renewal loop exits. The lease will eventually expire server-side.

### Signals (pub/sub)

`SignalConn` provides a separate connection type for pub/sub messaging. Unlike locks and semaphores, signals use a background reader that demultiplexes push messages (`sig <channel> <payload>`) from command responses:

1. `connect()` opens a TCP connection and starts a background reader (asyncio task or daemon thread).
2. `listen(pattern)` subscribes to channels matching a NATS-style wildcard pattern. Optional `group` parameter enables queue-group load balancing.
3. `emit(channel, payload)` publishes a signal on a literal channel (no wildcards). Returns the number of listeners delivered to.
4. `unlisten(pattern)` removes a subscription.
5. `close()` / `aclose()` shuts down the background reader and closes the connection.

Signals are delivered to a queue (`asyncio.Queue` or `queue.Queue`) that can be consumed via iteration (`for sig in sc:` / `async for sig in sc:`). A `None` sentinel in the queue indicates the connection has been closed.

A standalone `sig_emit()` function is also available for fire-and-forget publishing on plain connections without the background reader overhead.

### Stats

The `stats()` function sends a `stats` command to the server and returns a JSON dict with the current server state: active connections, held locks (with owner, lease expiry, and waiter counts), active semaphores (with holder and waiter counts), and idle entries. This is a low-level function that operates on an existing connection.

### Release and cleanup

On `release()` or context manager exit:

1. The renewal loop is stopped.
2. A release command is sent to the server.
3. The TCP connection is closed.

If the client disconnects without releasing (crash, network failure), the server automatically releases the lock when the lease expires or on disconnect (if auto-release is enabled on the server).

**Safety nets:** All server responses are subject to a 1 MiB size guard to prevent unbounded memory usage from malformed responses. If a client is garbage collected without being properly closed, `__del__` closes the underlying socket/transport and emits a `ResourceWarning`.

## Sharding

When multiple servers are configured, the client uses a sharding strategy to deterministically map each lock key to a server. The default strategy uses `zlib.crc32` for stable hashing. See [Sharding](sharding.md) for details.

## Module structure

| Module | Description |
|---|---|
| `dflockd_client` | Top-level package with convenience re-exports (`AsyncDistributedLock`, `SyncDistributedLock`, etc.) |
| `dflockd_client._common` | Shared protocol helpers: `encode_lines()`, `parse_lease()`, `StatsResult`, `Signal`, response size limits |
| `dflockd_client.client` | Async client (`asyncio`-based), extends `_AsyncBase` |
| `dflockd_client.sync_client` | Sync client (`socket` + `threading`-based), extends `_SyncBase` |
| `dflockd_client.sharding` | Sharding strategy and defaults |
