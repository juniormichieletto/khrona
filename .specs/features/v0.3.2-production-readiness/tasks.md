# Tasks: v0.3.2 - Production Readiness Hardening

## Phase 1: Project State Alignment
- [x] **Task 1.1:** Update `.specs/project/ROADMAP.md` so the Ktor quick-start and graceful shutdown items are marked consistently.
- [x] **Task 1.2:** Update `.specs/project/STATE.md` so resolved blockers are removed and remaining blockers match current code.

## Phase 2: JDBC Dispatcher Isolation
- [x] **Task 2.1:** Add a failing test that proves JDBC store work can be isolated from the caller dispatcher.
- [x] **Task 2.2:** Add an injectable JDBC dispatcher to `JdbcJobStore`, defaulting to `Dispatchers.IO`.
- [x] **Task 2.3:** Wrap JDBC store operations in the configured dispatcher without moving connection or transaction state across contexts.
- [x] **Task 2.4:** Verify H2, PostgreSQL, MySQL, and Oracle store tests still pass.

## Phase 3: Lease And Heartbeat Configuration
- [x] **Task 3.1:** Add failing scheduler tests for configurable lease duration and heartbeat interval.
- [x] **Task 3.2:** Add `executionLeaseDuration` and heartbeat configuration to `KhronaConfig` plus Kotlin duration DSL helpers.
- [x] **Task 3.3:** Replace hard-coded scheduler lease and heartbeat timing with validated config values.
- [x] **Task 3.4:** Add validation tests for non-positive lease duration, non-positive heartbeat interval, and heartbeat interval greater than or equal to lease duration.

## Phase 4: Bounded Polling And Claiming
- [x] **Task 4.1:** Add failing tests proving a scheduler poll processes no more than `pollBatchSize` eligible executions.
- [x] **Task 4.2:** Add bounded eligible execution listing to the `JobStore` contract while preserving existing store behavior where practical.
- [x] **Task 4.3:** Implement bounded listing in `MemoryJobStore`.
- [x] **Task 4.4:** Implement bounded JDBC queries for H2, PostgreSQL, MySQL, and Oracle.
- [x] **Task 4.5:** Add tests for backlog batching and ordered processing by `scheduled_at`.

## Phase 5: Retry Policy Hardening
- [x] **Task 5.1:** Add failing tests for invalid retry policy values.
- [x] **Task 5.2:** Add failing tests for fractional backoff factors with jitter disabled.
- [x] **Task 5.3:** Validate `maxAttempts`, `initialDelay`, `maxDelay`, `factor`, and `jitter` before jobs are accepted.
- [x] **Task 5.4:** Fix `RetryPolicy.calculateDelay` so fractional factors are preserved and capped correctly.

## Phase 6: Payload Serialization Fail-Fast
- [x] **Task 6.1:** Add failing JDBC tests for unsupported top-level payloads, unsupported nested payloads, and non-string map keys.
- [x] **Task 6.2:** Replace top-level unsupported payload `toString()` fallback with an explicit exception.
- [x] **Task 6.3:** Replace nested unsupported payload and map-key coercion fallback with explicit exceptions that identify the invalid path.
- [x] **Task 6.4:** Update README payload documentation to explain supported JSON-compatible payload values.

## Phase 7: Final Verification
- [x] **Task 7.1:** Run `./gradlew clean test`.
- [x] **Task 7.2:** Update `.specs/project/ROADMAP.md` and `.specs/project/STATE.md` to mark completed production-readiness tasks.
- [x] **Task 7.3:** Record any deferred non-blocking operational improvements separately from production-readiness blockers.

## Verification
- [x] JDBC store operations run on the configured dispatcher.
- [x] Scheduler leases use configured duration values.
- [x] Heartbeats use configured or validated derived intervals.
- [x] Invalid lease and heartbeat settings fail before scheduler runtime.
- [x] A single poll processes at most the configured batch size.
- [x] Memory, H2, PostgreSQL, MySQL, and Oracle bounded polling behavior is covered.
- [x] Invalid retry policy settings are rejected.
- [x] Fractional retry backoff factors produce correct delays when jitter is disabled.
- [x] Unsupported JDBC payload values fail fast instead of storing `toString()`.
- [x] README documents supported JDBC payload values.
- [x] `./gradlew clean test` passes.
