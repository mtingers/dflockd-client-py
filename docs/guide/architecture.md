# Architecture

## Overview

The dflockd-client library provides async and sync Python clients for [dflockd](https://github.com/mtingers/dflockd), a distributed lock server. Both clients communicate with the server over TCP using a line-based UTF-8 protocol.

```
┌─────────────────────────────────┐
│        Your application         │
│                                 │
│  ┌───────────┐  ┌───────────┐  │
│  │  Async    │  │  Sync     │  │
│  │  Client   │  │  Client   │  │
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

When `acquire()` or `enqueue()` is called, the client:

1. Selects a server using the sharding strategy (based on the lock key).
2. Opens a TCP connection to that server.
3. Sends the lock request over the wire.

### Lock acquisition

- **Single-phase (`acquire`)** — sends a lock request with a timeout. The server grants the lock immediately if free, or enqueues the client in FIFO order. The call blocks until the lock is granted or the timeout expires.
- **Two-phase (`enqueue` + `wait`)** — splits acquisition into two steps. `enqueue()` joins the queue and returns immediately with `"acquired"` or `"queued"`. `wait()` blocks until the lock is granted. This allows application logic (e.g. notifying an external system) between joining the queue and blocking.

### Background renewal

Once a lock is acquired, the client starts a background renewal loop:

- **Async client** — an `asyncio.Task` that sends renew requests at `lease * renew_ratio` intervals.
- **Sync client** — a daemon `threading.Thread` that does the same.

If renewal fails (server unreachable, lease already expired), the client logs an error and sets `token = None`.

### Release and cleanup

On `release()` or context manager exit:

1. The renewal loop is stopped.
2. A release command is sent to the server.
3. The TCP connection is closed.

If the client disconnects without releasing (crash, network failure), the server automatically releases the lock when the lease expires or on disconnect (if auto-release is enabled on the server).

## Sharding

When multiple servers are configured, the client uses a sharding strategy to deterministically map each lock key to a server. The default strategy uses `zlib.crc32` for stable hashing. See [Sharding](sharding.md) for details.

## Module structure

| Module | Description |
|---|---|
| `dflockd_client.client` | Async client (`asyncio`-based) |
| `dflockd_client.sync_client` | Sync client (`socket` + `threading`-based) |
| `dflockd_client.sharding` | Sharding strategy and defaults |
