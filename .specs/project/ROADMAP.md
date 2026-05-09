# ROADMAP — Khrona

## Next Tasks
- [ ] Implement Android SQLite Store (v0.6)
- [ ] Implement Admin API & Dashboard (v0.4)
- [ ] Implement Metrics (Micrometer/OpenTelemetry) (v0.4)
- [ ] Add lock inspection capabilities (v0.4)

## v0.1: Core & In-Memory
- [x] Project Setup & Module Structure
- [x] Core Domain Models (JobDefinition, JobExecution, etc.)
- [x] Kotlin DSL for Job Definition
- [x] In-Memory Job Store (`MemoryJobStore`)
- [x] Basic Scheduler Runtime (Coroutine-native)
- [x] Interval & Cron Triggers (In-Memory)
- [x] Startup Trigger Support (Basic implementation via initial scheduling)
- [x] Ktor Plugin Integration (Lifecycle)

## v0.2: Persistence & Reliability
- [x] JDBC / Postgres Persistence
- [x] One-time Jobs (Persistent)
- [x] Transactional Enqueue
- [x] Retry Policies & Backoff
- [x] Dead-Letter Support

## v0.3: Distributed & Coordination
- [x] Multi-node Claiming (Lease Model)
- [x] Distributed Locking (`FORBID` policy)
- [x] Stale Worker Recovery

## v0.3.1: Reliability Hardening
- [x] Fix JDBC Handler Registry (JobDefinition.handler loss)
- [x] Resilient Recurring Schedules (Next run after terminal failure)
- [x] Execution Timeouts Enforcement
- [x] Observable API (Suspendable register/trigger)
- [x] Structured JSON Payloads
- [x] JDBC Schema Optimization (Indexing)
- [x] Fail-fast JDBC Migration
- [x] Safe REPLACE Ordering (claim before supersede)

## v0.3.2: Production Readiness Hardening
- Implementation plan: `.specs/features/v0.3.2-production-readiness/`
- [x] Fix Ktor quick-start/API mismatch so documented setup compiles as written
- [x] Isolate blocking JDBC operations from coroutine scheduler/application dispatchers
- [x] Add graceful scheduler shutdown with bounded wait for active executions
- [x] Mark or release owned `CLAIMED`/`RUNNING` executions during shutdown instead of relying only on lease expiry
- [x] Make execution lease duration configurable
- [x] Make heartbeat interval configurable or derive it from validated lease settings
- [x] Add bounded polling/claiming so large backlogs do not load every eligible execution each poll
- [x] Validate retry policy inputs (`maxAttempts`, delays, factor, jitter)
- [x] Fix fractional retry backoff factor handling
- [x] Fail fast or require explicit serialization for unsupported JDBC payload types (no silent fallback)
- [x] Hardened pollBatchSize validation
- [x] Completed JDBC dispatcher isolation (including lazy dialect resolution and suspend migrate)

## v0.4: Ops & Visibility
- [ ] Admin API & Routes
- [ ] Metrics & Tracing (Micrometer/OpenTelemetry)
- [x] Misfire Policies
- [ ] Lock Inspection

## v0.5: Hardening
- [ ] Testkit (Virtual Time)
- [ ] Focused shutdown and cancellation contract tests
- [ ] Harden claimed-before-active shutdown edge so newly claimed executions cannot rely only on lease expiry
- [ ] Documentation & Samples
- [ ] Performance Tuning
- [ ] Timezone-aware cron scheduling
- [ ] Dead-letter queue tooling enhancements for inspection, replay, and cleanup
- [ ] Job pause/disable support
- [ ] Persisted job reconciliation for jobs removed from code
- [ ] Cross-Database Adaptive Delay Scheduler
- [ ] Adaptive scheduler configuration docs

## v0.6: Android SQLite Store
- Implementation plan: `.specs/features/v0.6-android-sqlite/`
- [ ] Add `khrona-store-android-sqlite` module
- [ ] Implement Android-compatible SQLite `JobStore`
- [ ] Add versioned schema and migrations
- [ ] Add Android SQLite store contract tests
- [ ] Document Android lifecycle and WorkManager integration boundaries

## Future Considerations
- **Cross-Database Adaptive Delay Scheduler:** Replace fixed idle polling with a hybrid adaptive loop that sleeps until the next known execution, stale recovery deadline, or a configurable max polling interval. The baseline must work across Memory, H2, PostgreSQL, MySQL, and Oracle without database-specific notifications. Native database wake-up mechanisms may be added later as optional optimizations.
- **Shutdown contract hardening:** Add focused tests for timeout, shutdown cancellation, cancellation propagation, and the claimed-before-active edge so the documented lifecycle remains enforceable.
- **Timezone support:** Add timezone-aware cron scheduling while preserving UTC as the portable default behavior.
- **Dead-letter queue tooling:** Expand current terminal `DEAD_LETTERED` status into operator-friendly inspection, replay, and cleanup workflows.
- **Job pause/disable:** Allow jobs to be temporarily suppressed without removing their definitions from code or storage.
- **Persisted job reconciliation:** Detect persisted jobs whose code-defined handler is no longer registered, then provide explicit disable, tombstone, cleanup, or migration workflows instead of leaving stale executions to be skipped forever.
- **Android OS wake-up integration:** Android SQLite persists Khrona state, but exact alarms, foreground services, and WorkManager orchestration should remain app-level integration points unless a later Android integration module is justified.
- **Dynamic Worker Sizing:** Automatically adjust the number of concurrent execution coroutines based on load.
