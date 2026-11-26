# Observability and Health Tasks (`OBS-1`)

Follow strict TDD (`RED -> GREEN -> REFACTOR`) for each task and keep all code, comments, and documentation in English.

## Task 1: Core Observability SPI & Error Diagnostics (`khrona-core`)

- [x] Add failing unit tests for `ErrorDiagnosticFormatter` (non-null message, null/blank message preserving exception class name, truncation at max length, custom redaction formatter) and `CompositeKhronaObserver` (exception isolation across callbacks and `aroundExecution`).
- [x] Add `Observability.kt` in `khrona-core` (`ScheduleReason`, `ExecutionOutcome`, `HeartbeatFailureReason`, `ErrorDiagnosticFormatter`, `KhronaObserver`, `CompositeKhronaObserver`).
- [x] Extend `KhronaConfig` with `observer`, `errorFormatter`, `healthPollFailureThreshold`, and `deadLetterSamplingInterval` (plus validation).
- [x] Verify `:khrona-core:test` passes.

## Task 2: Store Health & Execution Count Contract

- [x] Add failing `JobStoreContract` tests for `checkHealth()` (healthy `UP` with non-negative latency) and `countExecutions(query)` (job ID, status set, empty status set, scheduled time bounds, and verifying `limit` does not cap total count).
- [x] Create `Health.kt` in `khrona-core` (`HealthStatus`, `StoreHealth`, `SchedulerHealth`).
- [x] Add `checkHealth()` and `countExecutions(query)` to `JobStore`.
- [x] Implement `checkHealth()` and `countExecutions(query)` in `MemoryJobStore`, `JdbcDialect` (`healthCheckSql` including `OracleDialect` `FROM DUAL`, `countExecutionsSql`), `JdbcJobStore`, `RedisJobStore`, and `MockJobStore`.
- [x] Add dialect SQL tests in `JdbcCoverageTest` and failure/outage `checkHealth()` tests for Memory, JDBC, and Redis.
- [x] Verify `:khrona-core:test`, `:khrona-store-memory:test`, `:khrona-store-jdbc:test`, and `:khrona-store-redis:test` pass.

## Task 3: Scheduler Instrumentation & Health State Machine

- [x] Add failing `Scheduler` tests covering:
  - scheduling lag, execution duration, outcomes (`SUCCESS`, `FAILED_RETRY`, `DEAD_LETTERED`, `MISFIRED`, `SUPERSEDED`, `CANCELLED`, `SHUTDOWN_CHECKPOINT`)
  - retries, heartbeat successes/failures (`LOST_OWNERSHIP_OR_EXPIRED`, `STORE_ERROR`, `LEASE_BUDGET_EXCEEDED`), stale recovery, lock contention, retention cleanup, DLQ size sampling, and shutdown duration
  - observer exception isolation (throwing observer never fails job or stops scheduler)
  - `aroundExecution` coroutine context propagation
  - `ErrorDiagnosticFormatter` persisting exception class name when `message` is null
  - `Scheduler.health()` lifecycle transitions (`STOPPED` -> `STARTING` -> `UP` -> `DEGRADED`/`DOWN` during store outage -> `UP` on recovery -> `STOPPED`).
- [x] Instrument `Scheduler.kt` and implement `Scheduler.health()` and `ObservableJobStore`.
- [x] Verify `:khrona-core:test` and H2 integration tests pass.

## Task 4: `khrona-observability` Module & `MicrometerKhronaObserver`

- [x] Wire `khrona-observability` in `settings.gradle.kts`, `gradle/libs.versions.toml`, and `khrona-observability/build.gradle.kts` (`compileOnly` Micrometer & OpenTelemetry).
- [x] Add failing tests for `MicrometerKhronaObserver` using `SimpleMeterRegistry` (every meter emitted accurately, plus explicit assertion that tags never contain `executionId`, `correlationId`, `workerId`, or exception message text).
- [x] Implement `MicrometerKhronaObserver`.
- [x] Verify `:khrona-observability:test` passes.

## Task 5: `OpenTelemetryKhronaObserver`

- [x] Add failing tests for `OpenTelemetryKhronaObserver` using `InMemorySpanExporter` and `InMemoryMetricReader` (span creation, span attributes, coroutine context propagation across suspension points, error recording, bounded metric attributes, and shutdown checkpoint behavior).
- [x] Implement `OpenTelemetryKhronaObserver`.
- [x] Verify `:khrona-observability:test` passes.

## Task 6: CI Gates, Ktor E2E, Docs & Full Verification

- [x] Add `khrona-observability` to Pitest configuration (`build.gradle.kts`, `check_coverage.py`, `check_mutation_coverage.py`).
- [x] Extend `KtorGeneratorDockerE2ETest` to include `khrona-observability` (`MicrometerKhronaObserver`), verify successful/failing job metrics, and exercise host-owned `/health` transitions during a controlled store outage and recovery.
- [x] Update `README.md`, `docs/1.0-readiness.md` (`OBS-1` checklist), `.specs/project/ROADMAP.md`, and `.specs/project/STATE.md` in English (without bumping version or committing).
- [x] Run full verification:
  - `./gradlew clean test jacocoTestReport jacocoRootReport jacocoTestCoverageVerification jacocoRootCoverageVerification && python3 .github/scripts/check_coverage.py`
  - `./gradlew :khrona-core:pitest :khrona-ktor:pitest :khrona-store-memory:pitest :khrona-store-jdbc:pitest :khrona-observability:pitest && python3 .github/scripts/check_mutation_coverage.py`
  - `./gradlew :khrona-ktor:e2eTest`
  - `git diff --check`
