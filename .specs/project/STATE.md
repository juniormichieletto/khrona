# STATE — Khrona

## Decisions
- **Structure:** Transitioned from `.spec/SPEC.md` to `.specs/` structure for TLC spec-driven workflow.
- **Guarantee:** Explicitly settled on **at-least-once** execution; removed "exactly-once" to avoid distributed systems fallacies.
- **Storage:** `MemoryJobStore` is a first-class citizen for single-instance/dev/test use cases.
- **v0.1 Completion:** Implemented core engine, in-memory store, and Ktor integration.
- **v0.2 Completion:** Implemented JDBC/Postgres store, retries, and durable scheduling.
- **v0.3 Completion:** Implemented lease-based claiming, distributed locking (FORBID), and stale worker recovery.
- **v0.3.1 Hardening:** Implemented handler registry, resilient recurring schedules, enforced timeouts, suspendable APIs, structured JDBC payloads, fail-fast migrations, and safer REPLACE ordering.
- **v0.5.1 Release:** Implemented automatic execution retention and cleanup across all stores (JDBC, Redis, Memory), supported Kotlin Duration throughout the DSL with full backward compatibility, and updated documentation with runnable, self-contained examples.
- **v0.6.0 Heartbeat Fencing:** Added `workerId` ownership validation and unexpired-lease enforcement to `JobStore.heartbeat` across Memory, JDBC, and Redis, with atomic Lua heartbeat renewal and lease-preserving status transitions in Redis.
- **v0.7.0 Production Readiness Hardening:** Resolved all P0 and P1 lifecycle, concurrency, and persistence defects (atomic completion, initial seeding, query-side candidate filtering, lease budgeting, bounded concurrency, poison row isolation, and UTC/MySQL timestamp fixes).

## Blockers
- None. v0.3.2 hardening addresses previously identified production-readiness blockers, including validation and lifecycle isolation gaps found in review.

## Todos
- [x] Map existing codebase (if any) to `.specs/codebase/`.
- [x] Initialize the Gradle/Kotlin project structure.
- [x] Implement JDBC/Postgres Store (v0.2)
- [x] Implement Multi-node Claiming (v0.3)
- [x] Implement Reliability Hardening (v0.3.1)
- [x] Add Production Readiness Hardening implementation plan (v0.3.2)
- [x] Implement Production Readiness Hardening (v0.3.2)
- [x] Add Android SQLite Store implementation plan (v0.7)
- [x] Add Redis Store implementation plan (v0.4)
- [x] Implement Redis Store (v0.4)
- [x] Implement Core Job Management API & ExecutionContext
- [ ] Implement Metrics & Lock Inspection (v0.5)
- [ ] Implement Android SQLite Store (v0.7)

## Current Implementation Status
- **Redis Store (v0.4 -> v0.6.0):** Production-ready (graduated in v0.6.0). Uses Lettuce, `RedisJobStoreConfig`, `RedisJobStoreConfig.clusterNamespace(namespace)`, `KhronaRedisOomException`, single-round-trip atomic Lua scripts across `saveExecution`, `claimExecution`, `heartbeat`, `updateExecutionStatus`, `resetExpiredExecutions`, `listEligibleExecutions`, `isLockHeld`, `supersedeExecutionsByLockKey`, and chunked `cleanupCompletedExecutions` (with `$namespace:cleanup:lock` and automatic orphan/stale index self-healing), verified by `RedisJobStoreTest`, shared `JobStoreContract`, Dockerized Ktor E2E tests, and 1/2/15-node cluster drain benchmarks. `./gradlew clean test` passes.
- **Redis remaining work:** Optional command-latency observability hook (Micrometer/OpenTelemetry) planned alongside core metrics.
## Next Tasks
- [x] Implement Core Job Management API (v0.5)
- [ ] Implement Metrics (Micrometer) (v0.5)
- [ ] Add lock inspection capabilities (v0.5)
- [ ] Implement Android SQLite Store (v0.7)
- [ ] Add shutdown/cancellation contract hardening to a future reliability release
- [ ] Add timezone support, DLQ tooling enhancements, and persisted job reconciliation to future production features


## Deferred Ideas
- Payload versioning/evolution (tracked in Future Considerations).
- Host-owned management security patterns for applications that expose Khrona operations.
- Dynamic Lock Keys.
- Shutdown/cancellation contract tests and claimed-before-active edge hardening.
- Timezone-aware cron scheduling.
- Dead-letter queue inspection, replay, and cleanup tooling.
- Job pause/disable support.
- Persisted job reconciliation for jobs removed from code, including disable/tombstone/cleanup workflows.
- Cross-database adaptive scheduler delay with bounded fallback polling.
- Optional database-native wake-up mechanisms after the portable adaptive loop exists.
- Android OS wake-up integration beyond storage, such as exact alarms or first-class WorkManager orchestration.
- Redis Cluster-specific sharding and Pub/Sub wake-up optimizations after the baseline Redis store exists.

## PR #11 Follow-up Fixes (2026-11-15)

- Completed conservative request-start lease budgeting with a monotonic watchdog bound, original-store claim lifecycle binding, and starvation-free filtering for legacy JDBC dialects.
- Added ten behavioral regression/boundary tests. Verified 403 unit/integration tests, 13 performance tests, 3 Ktor E2E tests, all coverage gates, and 91.61% aggregate mutation coverage.
- Details and remaining operational limits: `.specs/quick/001-pr11-followup/SUMMARY.md`.

## Shutdown Snapshot Follow-up (2026-11-15)

- Fixed the last-claim removal race in `Scheduler.stop()` by copying the concurrent claim map through traversal instead of Kotlin's size-based singleton conversion. Original-store binding and owner-fenced release remain intact.
- Added a deterministic regression that reproduced `NoSuchElementException` before the fix and passes afterward. The Oracle test's timeout and parallelism assertions remain unchanged.
- Verified a clean 404-test suite, 13 performance tests, 3 Ktor E2E scenarios, all coverage gates, and all mutation gates (91.42% aggregate; core 90.08%). All tests passed with zero failures/errors/skips.

## 1.0 Readiness Notes

- The six remaining readiness areas and their acceptance evidence are tracked in `docs/1.0-readiness.md`, linked from the roadmap and README.
- The existing Dockerized Ktor generator suite is the consumer integration baseline for Memory, PostgreSQL, and Redis. New API, observability, operator-control, upgrade, and failure scenarios should extend it alongside focused contract/container tests.
- Release numbering remains a separate decision; reconcile the proposed reliability release with the existing Android SQLite v0.7 allocation before tagging.

## Core Job Management API & ExecutionContext (2026-11-24)

- Implemented `JobStore.listExecutions(query)` across Memory, JDBC (PostgreSQL, MySQL, Oracle, H2), Redis, and `MockJobStore`, plus terminal `ExecutionStatus.CANCELLED` and `JobDefinition.paused` persistence.
- Added `Scheduler` management APIs (`listJobOverviews`, `getJobOverview`, `listJobHistory`, `listExecutions`, `startJob`, `pauseJob`, `resumeJob`, `stopExecution`, `stopJob`) and `ExecutionContext` (`executionId`, `jobId`, `attempt`, `scheduledAt`, `payload`, `correlationId`, `checkpoint()`).
- Updated `JobHandler` to `suspend ExecutionContext.(payload: Any?) -> Unit`; standard DSL lambdas remain source-compatible, while callers holding pre-typed `suspend (Any?) -> Unit` references should update them to `JobHandler`.
