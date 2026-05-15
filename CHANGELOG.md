# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
