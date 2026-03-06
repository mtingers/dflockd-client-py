# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

