# Architecture

The client is organised in three layers, built around the line-based
TCP protocol described in the [dflockd
docs](https://github.com/mtingers/dflockd):

```
┌──────────────────────────────────────┐
│  High-level: DistributedLock,        │  acquire() / release() /
│              DistributedSemaphore    │  enqueue() / wait()
│                                      │  + background lease renewal
├──────────────────────────────────────┤
│  Transport: SyncConn / AsyncConn     │  conn.command(cmd, key, arg)
├──────────────────────────────────────┤
│  Wire: _protocol module              │  encode lines, parse responses
└──────────────────────────────────────┘
```

Each layer is independently usable. Tests cover the wire layer with no
sockets, the transport layer with a fake `Conn`, and the high-level
types against a running server.

## Wire protocol layer (`_protocol`)

Pure functions. No sockets, no asyncio. Everything in this layer maps
bytes ↔ Python types:

- **Validators** — reject keys, tokens, timeouts, and limits that would
  desynchronise the server's framing.
- **Encoders** — `encode_lines("l", "k", "5") → b"l\\nk\\n5\\n"`,
  plus `make_*_arg` helpers that compose validate + build per command.
- **Decoders** — `parse_grant_response("ok abc 30") → ("abc", 30)`,
  `parse_enqueue_response("queued") → ("queued", None, None)`. Each
  decoder maps known wire codes to the matching sentinel exception.

Status codes from the server map to specific exception classes:

| Wire code                  | Exception              |
|----------------------------|------------------------|
| `error_auth`               | `AuthError` (or `PermissionError` during the auth handshake) |
| `error_max_locks`          | `MaxLocksError`        |
| `error_max_waiters`        | `MaxWaitersError`      |
| `error_limit_mismatch`     | `LimitMismatchError`   |
| `error_not_enqueued`       | `NotQueuedError`       |
| `error_already_enqueued`   | `AlreadyQueuedError`   |
| `error_lease_expired`      | `LeaseExpiredError`    |
| `error_draining`           | `DrainingError`        |
| `timeout` (acquire/wait)   | `DflockdTimeoutError`  |
| anything else              | `DflockdError`         |

## Transport layer (`SyncConn` / `AsyncConn`)

Each `Conn` wraps a single TCP (or TLS) connection. The only I/O method
is `command(cmd, key, arg, *, read_timeout)`:

1. Encode and write the 3-line frame.
2. Read exactly one response line, capped at 1 MiB.
3. Return the response string. Decoding is the caller's job.

`SyncConn` uses `socket.settimeout` per-call so long-poll commands
(`acquire`/`wait`) and short ones (`renew`) share one connection.
`AsyncConn` uses `asyncio.wait_for` for the same purpose.

If a `command()` call raises (timeout, network error, server protocol
violation), the connection is unsafe to reuse — close it and dial a new
one. The high-level types do this automatically.

## High-level layer

`DistributedLock` and `DistributedSemaphore` own a single connection,
manage the lease-renewal worker, and turn the two-phase API into a
familiar `acquire()`/`release()` shape. The classes are dataclasses,
which means construction is just keyword arguments — no boilerplate.

### Single-phase acquire

```
acquire():
  1. close any prior connection / renew worker  (reset for new attempt)
  2. dial server, authenticate if auth_token set
  3. send `l <key> <timeout> [<lease_ttl>]`
  4. on grant: store token+lease, start renewal worker
  5. on timeout: close connection, return False
  6. on error: close connection, raise the matching sentinel
```

### Two-phase acquire

```
enqueue():
  1-2. as above
  3. send `e <key> [<lease_ttl>]`
  4. on "acquired" (fast path): store token+lease, start renewal, return "acquired"
  5. on "queued": keep the connection, return "queued"

wait(timeout):
  1. if a token was already acquired in enqueue(): return True (no I/O)
  2. send `w <key> <timeout>` on the existing connection
  3. on grant: store token+lease, start renewal, return True
  4. on timeout: close connection, return False
```

The same `Conn` is used for `enqueue` and `wait`: the server tracks
queue position by per-connection `connID`, not by token, so the calls
must share a connection.

### Background renewal

Once a lock is acquired, a worker (daemon thread / asyncio task) sleeps
for `lease * renew_ratio` seconds, then sends a renew. The remaining
seconds returned by the server become the next interval.

Concurrency: `_io_lock` serialises the renewal worker against
`release()`. The renewal worker checks the in-flight flag on each tick,
so a `release()` issued while a renew is in flight is processed cleanly
once the renew completes.

### Cleanup

| Call               | Stops renewal | Sends release | Closes connection |
|--------------------|---------------|---------------|-------------------|
| `release()`        | yes           | yes           | yes               |
| `close()` / `aclose()` | yes       | no            | yes (server auto-releases on disconnect) |
| context-manager exit | yes         | yes (via `release()`) | yes       |
| `__del__` (GC)     | best-effort   | no            | best-effort, with `ResourceWarning` |

Releasing without sending the release (`close()` / GC) relies on
dflockd's `--auto-release-on-disconnect` (on by default) to free the
slot. Explicit `release()` is faster because the server doesn't wait for
the TCP teardown.

## Sharding

When `servers=` has more than one entry, the client picks a server using
the configured `sharding_strategy(key, num_servers) → index`. The
default is CRC-32 (`stable_hash_shard`) — deterministic across processes
and matching the Go and TypeScript clients. See [Sharding](sharding.md).

## Module layout

| Module                       | Contents |
|------------------------------|----------|
| `dflockd_client`             | Top-level re-exports of every public symbol |
| `dflockd_client.errors`      | Sentinel exception classes |
| `dflockd_client.sharding`    | `ShardingStrategy`, `stable_hash_shard`, `DEFAULT_SERVERS` |
| `dflockd_client._protocol`   | Pure wire protocol (encoders, decoders, validators) |
| `dflockd_client._sync`       | `SyncConn`, sync command functions, `DistributedLock`, `DistributedSemaphore` |
| `dflockd_client._async`      | Async equivalents |

The `_sync` and `_async` underscores are a hint that the high-level
types should be reached for first; the low-level functions are stable
but only useful when explicit connection management matters.
