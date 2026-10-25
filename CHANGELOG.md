# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Next release: **0.6.0** (minor release with breaking changes).

### Changed
- **Breaking — custom JobStore implementations:** `heartbeat(id: UUID, leaseDuration: Duration)` is replaced by `heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean`. Implementations and direct callers must update and recompile. Renew only when the execution is `CLAIMED`/`RUNNING`, the stored owner matches `workerId`, and the lease is still valid; validate and update atomically. See the [migration notes](README.md#upgrading-to-060).
- **Breaking — custom JdbcDialect implementations:** `JdbcJobStore.heartbeat` now binds four positional `?` parameters (`new expires_at`, `id`, `claimed_by`, `now`) instead of two. Custom `JdbcDialect.heartbeatSql()` implementations must update their SQL placeholders to avoid runtime parameter-binding errors.
- **Breaking — expired lease behavior:** A late heartbeat now returns `false` even if no other worker has reclaimed the execution. Previously it could revive an expired active lease. On a `false` response, the scheduler requests cooperative cancellation of the handler. GC pauses, slow storage, or outages that exhaust the renewal margin can therefore cancel otherwise healthy work. `heartbeatInterval` still defaults to half of `executionLeaseDuration`; size the lease for expected pauses and storage latency.
- Redis heartbeat renewal also requires the corresponding claimed/running sorted-set entry with an unexpired score (checked against both the caller timestamp and Redis server `TIME` at script execution). Missing lease-index entries cause renewal to fail without rebuilding the index.

### Fixed
- Redis heartbeat status validation, lease renewal, index update, and claim-key TTL renewal now execute in one Lua script, preventing a stale heartbeat from restoring a superseded execution or reviving a lease after an in-flight delay past expiry.
- Redis `updateExecutionStatus` now preserves the current sorted-set lease score and claim-lock TTL inside the Lua script instead of overwriting them with a pre-read client snapshot, preventing `CLAIMED` → `RUNNING` transitions from clobbering concurrent heartbeat renewals.
- Memory, JDBC, and Redis heartbeats reject renewal by a different worker after reclamation. This does not yet fence final status writes or distinguish successive claims by the same worker; see the [remaining fencing work](.specs/features/v0.3-distributed/design.md#remaining-fencing-work).

## [0.5.1] - 2026-10-01

### Added
- **Execution Retention & Cleanup:** Added automatic bounded execution retention and cleanup across core scheduler and all storage backends (JDBC, Memory, Redis) with configurable `retention { ... }` DSL block and `idx_khrona_executions_status_completed` index.
- **Kotlin Duration DSL Support:** Supported `kotlin.time.Duration` throughout the DSL (`RetentionPolicy`, `JobBuilder.timeout`, `RetryPolicyBuilder`, `IntervalTrigger`) with full backward compatibility for `java.time.Duration`.

### Changed
- **Documentation & Quick Start:** Streamlined README installation and added complete, runnable copy-paste snippets for both Ktor and Standalone Kotlin applications.
- **Architecture Diagrams:** Updated component and sequence/flowchart diagrams in `README.md` and `.specs/codebase/ARCHITECTURE.md` to reflect the retention and cleanup lifecycle.

## [0.5.0] - 2026-06-16

### Added
- **GitHub Releases:** Added JReleaser-based GitHub release creation with generated release notes and no attached JAR assets.
- **Release Backfill Tooling:** Added a dry-run-first helper for backfilling historical GitHub releases from existing tags.
- **Release Process Notes:** Documented the normal release flow, historical backfill process, and verification commands in `docs/release-process.md`.
- **Flyway Setup Docs:** Added README guidance for using the built-in JDBC schema with Flyway-managed migrations.

### Changed
- Reorganized README getting-started and architecture sections for clearer onboarding.
- Serialized JDBC tests to reduce contention in integration test runs.

### Fixed
- Prevented duplicate schedules when concurrent schedulers handle ignored misfires.

## [0.4.0] - 2026-05-15

### Added
- **Redis Store (Experimental):** New `khrona-store-redis` module providing high-performance, low-latency job storage and distributed coordination.
- **Docker Helper Scripts:** Added `docker-start.sh` and `docker-stop.sh` for easy management of the local Redis development environment.
- **Performance Benchmarks:** Conducted and documented polling performance across all storage engines (PostgreSQL, MySQL, Redis, H2). See `POLLING_PERFORMANCE.md`.

### Changed
- Updated documentation to reflect the Redis Store is ready for use but marked as experimental for production workloads.
- Simplified local development setup instructions in `README.md`.

## [0.3.3] - 2026-05-14

### Fixed
- **Atomic REPLACE:** Improved reliability of job replacement in distributed environments by ensuring claim-before-supersede ordering.
- **Timeouts:** Enhanced enforcement of execution timeouts.

## [0.3.2] - 2026-05-13

### Changed
- **Production Hardening:** Completed major reliability updates including JDBC dispatcher isolation, graceful shutdown, and bounded polling.

## [0.3.1] - 2026-05-10

### Fixed
- **Handler Registry:** Fixed a critical issue where `JobDefinition.handler` could be lost in certain JDBC configurations.
- **Resilient Schedules:** Improved recurring schedule resilience after terminal execution failures.

## [0.3.0] - 2026-05-01

### Added
- **Distributed Coordination:** Introduced multi-node claiming via a lease model and distributed locking with the `FORBID` policy.

## [0.2.0] - 2026-04-15

### Added
- **Persistence:** Introduced JDBC / Postgres storage support.
- **Reliability:** Added one-time jobs, transactional enqueue, retry policies, and dead-letter support.

## [0.1.0] - 2026-04-01

### Added
- **Core Engine:** Initial release with coroutine-native runtime, Kotlin DSL, and Ktor integration.
- **In-Memory Store:** Basic `MemoryJobStore` for single-instance use.
