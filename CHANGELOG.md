# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

