# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v2.1.0] - 2026-05-10

### Added

- `fence_from_token(token)` — decodes the 64-bit monotonic fence prefix from a grant token, mirroring the Go client's `FenceFromToken`. dflockd grant tokens are `<16-hex monotonic prefix><16-hex random salt>`; the prefix strictly increases on every grant from a server instance, so a token doubles as a fencing token for downstream resources. Exposed as an `int` and re-exported from the top-level `dflockd_client` package.

## [v2.0.1] - 2026-05-06

### Fixed

- Enforced protocol maximum bounds on server-supplied lease and renewal values before scheduling client-side renewals.
- Normalized invalid UTF-8 responses from sync and async transports into `RuntimeError`, matching the malformed-response error contract.
- Enforced sync response size limits on raw bytes before UTF-8 decoding.
- Applied async command timeouts to both write drain and response reads while preserving external cancellation semantics.
- Hardened sync renewal cleanup so stale renewal workers cannot update a reused lock or semaphore after shutdown.
- Expanded unit coverage for protocol boundary validation, malformed transport responses, cancellation, and renewal races.

## [v2.0.0] - 2026-05-05

### Changed

- Rewrote the client for the dflockd v2 wire protocol, with parallel sync and async APIs for locks and counting semaphores.
- Simplified the public surface around `SyncDistributedLock`, `AsyncDistributedLock`, `SyncDistributedSemaphore`, `AsyncDistributedSemaphore`, `SyncConn`, and `AsyncConn`.
- Updated stats decoding for the v2 server shape; pub/sub stats fields are no longer present.

### Fixed

- Hardened sync and async lifecycle cleanup around cancellation, release, renewal failure, and instance reuse.
- Closed sync transports after post-write command failures so stale responses cannot corrupt the next command.
- Validated high-level keys, acquire timeouts, lease TTLs, semaphore limits, and custom shard indexes before use.

### Removed

- Removed the v1 pub/sub signal client APIs; dflockd v2 focuses this package on distributed locks and semaphores.

[v2.1.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v2.1.0
[v2.0.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v2.0.1
[v2.0.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v2.0.0

## [v1.9.0] - 2026-03-13

### Added

- Ping heartbeat for `SignalConn` (async and sync) to prevent idle signal listener connections from being disconnected by the server's read timeout (requires dflockd v1.14.0+)
- `heartbeat_interval_s` parameter on both `AsyncSignalConn` and `SyncSignalConn` (default 15 seconds, set to 0 to disable)

[v1.9.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.9.0

## [v1.8.11] - 2026-03-10

### Fixed

- Async `SignalConn._read_loop` did not catch `ValueError`, causing `UnicodeDecodeError` from malformed server responses to become an unhandled exception in the background reader task (sync version already handled this correctly)
- `_AsyncBase.aclose()` and `SignalConn.aclose()` left stale `_reader`/`_writer`/`token` references if `CancelledError` was raised during `wait_closed()`, since `contextlib.suppress(Exception)` does not catch `BaseException` subclasses; state is now cleared before the cancellable await

[v1.8.11]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.11

## [v1.8.10] - 2026-03-08

### Docs

- Added missing `KW_ONLY` marker to both `SignalConn` dataclass definitions in API reference (async and sync)
- Updated stale `__version__` example in API reference

[v1.8.10]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.10

## [v1.8.9] - 2026-03-08

### Fixed

- `SignalConn.connect()` (async and sync) did not close the existing connection before reconnecting, leaking the old socket, reader task/thread, and file descriptors when called twice without an intervening `aclose()`/`close()`

[v1.8.9]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.9

## [v1.8.8] - 2026-03-08

### Fixed

- Race condition in async `SignalConn._send_cmd`: null check on `_writer` was outside `_cmd_lock`, allowing `aclose()` from another task to cause `AttributeError`
- Race condition in sync `SignalConn._send_cmd`: null check on `_sock` was outside `_cmd_lock`, allowing `close()` from another thread to cause `AttributeError`
- Misleading exception chain in sync `SignalConn._send_cmd`: `ConnectionError` raised inside `except queue.Empty` produced confusing chained traceback

[v1.8.8]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.8

## [v1.8.7] - 2026-03-07

### Docs

- Rewrote README as a concise entry point (343 → 91 lines): install, one example per feature, link to docs
- Removed duplicate parameter tables, auth/TLS sections, and stats/sharding examples from README (covered in docs site)
- Merged separate Authentication and TLS sections into one in async and sync client guides
- Condensed two-phase acquisition explanations to bullet points in client guides
- Replaced duplicate semaphore parameter tables with cross-references to lock parameters
- Removed "Query server stats" and "What happens under the hood" from quickstart (covered in examples and architecture)
- Tightened docs/index.md feature list

[v1.8.7]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.7

## [v1.8.6] - 2026-03-07

### Docs

- Added `KW_ONLY` marker to all dataclass definitions in API reference to accurately show keyword-only parameters
- Added missing `Signal`, `AsyncSignalConn`, `SyncSignalConn` to top-level import example in API reference
- Added keyword-only parameter notes to lock and semaphore parameter tables in client guides
- Fixed background renewal description: incorrectly stated "sets `token = None`" on failure; actually the renewal loop exits and the lease expires server-side

[v1.8.6]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.6

## [v1.8.5] - 2026-03-07

### Fixed

- Formatting fixes for CI

[v1.8.5]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.5

## [v1.8.4] - 2026-03-07

### Fixed

- `SignalConn.connect()` did not reset `_closed` flag or create fresh queues, preventing reconnection after `close()`/`aclose()`
- `None` sentinel silently dropped when signal queue was full, causing `for sig in sc:` / `async for sig in sc:` to hang forever
- Sync `_read_loop` blocked indefinitely on full `_resp_queue` (changed `put()` to `put_nowait()`)

[v1.8.4]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.4

## [v1.8.3] - 2026-03-06

### Docs

- Added signals documentation to README, quickstart, examples, client guides, API reference, and architecture docs

[v1.8.3]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.3

## [v1.8.1] - 2026-03-06

### Fixed

- Formatting fixes for CI compliance

[v1.8.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.1

## [v1.8.0] - 2026-03-06

### Added

- Signal (pub/sub) support: `AsyncSignalConn` and `SyncSignalConn` high-level classes with background reader for push delivery
- `listen(pattern, group=)` / `unlisten(pattern, group=)` for subscribing to channels with NATS-style wildcards (`*`, `>`)
- `emit(channel, payload)` for publishing signals on literal channels; returns delivery count
- Queue group support for load-balanced signal delivery (round-robin within a group)
- `Signal` NamedTuple (`channel`, `payload`) for received signals
- Low-level `sig_emit()` protocol function for fire-and-forget publishing on plain connections
- Iteration support: `async for sig in conn:` / `for sig in conn:` with clean termination on disconnect
- Context manager support (`async with` / `with`) for signal connections
- Signal test suite with unit and integration tests

[v1.8.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.8.0

## [v1.7.3] - 2026-03-06

### Fixed

- Missing GitHub release for v1.7.2 prevented automated PyPI publish; re-release with no code changes

[v1.7.3]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.7.3

## [v1.7.2] - 2026-03-06

### Fixed

- `stats()` protocol: send `_` placeholder instead of empty string as third argument to `encode_lines` (async and sync clients)

### Docs

- Corrected `acquire` and `sem_acquire` examples to use positional arguments

[v1.7.2]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.7.2

## [v1.7.1] - 2026-02-24

### Fixed

- Dead `readline` check that could never trigger; lease is now reset to `0` on `close()`

### Changed

- Unified lock and semaphore protocol functions via `cmd_prefix`, reducing code duplication

### Added

- Strict FIFO ordering tests for locks and semaphores (async and sync), verifying 5 waiters are granted in exact enqueue order

[v1.7.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.7.1

## [v1.7.0] - 2026-02-24

### Fixed

- Sync renew loop race: replace `_stop_event.clear()` with fresh `Event` to prevent lingering threads from ignoring stop signals
- Renew loop sends aggressive 1-second intervals when server returns lease=0 (now falls back to 30s)
- `__del__()` now closes leaked sockets/transports with `ResourceWarning` instead of silently leaking FDs
- Async `aclose()` bounds `wait_closed()` to 5s to prevent indefinite hangs
- Renew loop holds local references to stale connections after reconnect (now cleared in `finally`)
- Async readline missing 1 MiB length guard (now raises `RuntimeError` for oversized responses)
- Auth handshake missing timeout (now uses `connect_timeout_s`)
- Renew loop continues operating on stale connections after reconnect (added identity checks)
- `_stop_renew()` thread orphan when `_renew_thread` reference is stale
- Spurious "lock lost" log messages on normal shutdown
- Sync socket timeouts not set during renew (now scoped to 5s for renew operations)
- Sync `close()` stalls waiting for `_io_lock` held by renew loop (now interrupts via `sock.shutdown()`)
- Enqueue bounds checks missing for malformed server responses
- Release errors in `__exit__`/`__aexit__` could mask the original exception (now suppressed)
- Protocol arguments containing newlines could corrupt the wire format (now rejected by `encode_lines()`)
- `renew_ratio` outside (0, 1) not validated (now raises `ValueError`)

### Added

- Top-level package exports: `AsyncDistributedLock`, `AsyncDistributedSemaphore`, `SyncDistributedLock`, `SyncDistributedSemaphore`, `StatsResult`, `DEFAULT_SERVERS`, `ShardingStrategy`, `stable_hash_shard`, `__version__`
- Shared `_common` module with `encode_lines()`, `parse_lease()`, response size limits, and connect timeout constant
- Comprehensive resource cleanup tests (`test_resource_cleanup.py`)
- `DEFAULT_SERVERS` is now an immutable tuple

### Changed

- Refactored async and sync clients to share base classes (`_AsyncBase`, `_SyncBase`), eliminating code duplication

[v1.7.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.7.0

## [v1.6.2] - 2026-02-24

### Fixed

- Sync client leaks socket when calling `acquire()`/`enqueue()` on an already-connected instance (missing `close()` before reconnect)
- Sync client leaks raw socket when `wrap_socket()` or `makefile()` fails during `_connect()`
- Sync renew loop leaves socket open on renewal failure (now calls `close()` instead of only clearing the token)
- Async `aclose()` skips `wait_closed()` if `close()` raises (now each call is independently guarded)
- Malformed server lease values cause unhandled `ValueError` (now falls back to default)
- Malformed JSON in `stats()` response causes unhandled `JSONDecodeError` (now raises `RuntimeError`)

### Added

- Connect timeout (10 s) on sync `socket.create_connection()` to prevent blocking on unreachable hosts

[v1.6.2]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.6.2

## [v1.6.1] - 2026-02-24

### Added

- GitHub Actions workflow to publish to PyPI on new releases via trusted publishing

[v1.6.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.6.1

## [v1.6.0] - 2026-02-18

### Added

- `auth_token` parameter on `DistributedLock` and `DistributedSemaphore` in both async and sync clients for token-based authentication
- Auth handshake is sent automatically in `_connect()` when `auth_token` is set; raises `PermissionError` on failure
- Auth unit tests verifying `auth_token` defaults to `None` and can be set
- Auth integration tests gated behind `DFLOCKD_TEST_AUTH_TOKEN` and `DFLOCKD_TEST_AUTH_PORT` env vars
- Authentication documentation in README, API reference, client guides, and examples

[v1.6.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.6.0

## [v1.5.1] - 2026-02-18

### Fixed

- Add return types to async `_connect()` helpers for pyright type narrowing

[v1.5.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.5.1

## [v1.5.0] - 2026-02-18

### Added

- `ssl_context` parameter on `DistributedLock` and `DistributedSemaphore` in both async and sync clients for TLS connections
- `_connect()` helper in async `DistributedLock` and `DistributedSemaphore` to centralize connection logic
- TLS unit tests verifying `ssl_context` defaults to `None`
- TLS integration tests gated behind `DFLOCKD_TEST_TLS_PORT` env var
- TLS documentation in README, API reference, client guides, and examples

[v1.5.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.5.0

## [v1.4.0] - 2026-02-18

### Added

- `stats()` function in both async and sync clients for querying server state (connections, locks, semaphores, idle entries)
- Stats integration tests for both async and sync clients
- Documentation for stats in README, API reference, client guides, quickstart, examples, and architecture docs

### Changed

- Bumped version to v1.4.0 to align with dflockd server version

[v1.4.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.4.0

## [v1.1.0] - 2026-02-16

### Added

- `DistributedSemaphore` class in both async and sync clients, allowing up to N concurrent holders per key
- Semaphore protocol functions (`sem_acquire`, `sem_release`, `sem_renew`, `sem_enqueue`, `sem_wait`) in both async and sync clients
- Two-phase semaphore acquisition via `DistributedSemaphore.enqueue()` and `DistributedSemaphore.wait()`
- Semaphore integration and unit tests mirroring existing lock test structure
- Documentation for semaphores in README, API reference, client guides, quickstart, examples, and architecture docs

[v1.1.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.1.0

## [v1.0.1] - 2026-02-16

### Fixed

- Close existing connections before opening new ones in `acquire()`, `enqueue()`, and `__aenter__()`, preventing socket leaks on repeated calls
- Close socket on renew failure instead of only clearing the token, so the connection is properly cleaned up when a lock is lost
- Suppress `release()` errors in `__aexit__` to avoid masking the original exception from the `async with` block
- Examples import referenced the old `dflockd` package name instead of `dflockd_client`

### Changed

- Added documentation site link to README

[v1.0.1]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.0.1

## [v1.0.0] - 2026-02-15

### Changed

- Split from [dflockd](https://github.com/mtingers/dflockd) into a standalone client-only package (`dflockd-client`)
- Server has been ported to Go; this package now contains only the Python client
- Renamed package import from `dflockd` to `dflockd_client`
- Integration tests now require an external running dflockd server (configurable via `DFLOCKD_TEST_HOST` / `DFLOCKD_TEST_PORT` env vars, skipped if unavailable)
- Documentation rewritten to focus on client usage; server and wire protocol docs removed

### Removed

- Server code (`dflockd.server`)
- Server CLI (`dflockd` command)
- Server configuration docs, wire protocol docs
- TypeScript client (`ts/`)

[v1.0.0]: https://github.com/mtingers/dflockd-client-py/releases/tag/v1.0.0

## [v0.5.0] - 2026-02-14

### Added

- Two-phase lock acquisition with `e` (enqueue) and `w` (wait) protocol commands
- `enqueue()` and `wait()` module-level functions in async and sync clients
- `DistributedLock.enqueue()` and `DistributedLock.wait()` methods in async and sync clients
- Documentation for two-phase flow in client and examples docs

[v0.5.0]: https://github.com/mtingers/dflockd/releases/tag/v0.5.0

## [v0.4.1] - 2026-02-07

### Added

- `--auto-release-on-disconnect` / `--no-auto-release-on-disconnect` CLI flag

### Fixed

- `DFLOCKD_DFLOCKD_READ_TIMEOUT_S` env var typo in README (now `DFLOCKD_READ_TIMEOUT_S`)
- Server configuration env var names in README missing `DFLOCKD_` prefix
- `MAX_LOCKS` default in README corrected from `256` to `1024`

[v0.4.1]: https://github.com/mtingers/dflockd/releases/tag/v0.4.1

## [v0.4.0] - 2026-02-07

### Added

- Documentation site (MkDocs Material) with architecture, configuration, client, protocol, and sharding guides

### Fixed

- Pyright CI dependency
- Ruff dev dependency

### Changed

- Bump `actions/checkout` from 4 to 6
- Bump `actions/setup-python` from 5 to 6
- Bump `actions/upload-pages-artifact` from 3 to 4
- Bump `astral-sh/setup-uv` from 4 to 7
- Update `uv-build` requirement from >=0.9.28,<0.10.0 to >=0.9.28,<0.11.0

[v0.4.0]: https://github.com/mtingers/dflockd/releases/tag/v0.4.0

## [v0.3.0] - 2026-02-07

### Added

- Async client (`dflockd.client`) with `DistributedLock` context manager
- Sync client (`dflockd.sync_client`) with `DistributedLock` context manager
- Background lease renewal for both async and sync clients
- Multi-server sharding with `stable_hash_shard` (CRC-32)
- Custom sharding strategy support via `ShardingStrategy` callable
- Configurable `renew_ratio` for controlling renewal frequency
- CI workflow with linting, type checking, and tests
- GitHub Pages documentation deployment workflow

[v0.3.0]: https://github.com/mtingers/dflockd/releases/tag/0.3.0
