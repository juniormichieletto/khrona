# Khrona Project Review

Date: 2026-05-02

## Summary

Khrona is a promising early-stage Kotlin/Ktor scheduler library. The architecture is compact, the module split is sensible, and the project already has useful tests across the core scheduler, Ktor integration, in-memory storage, and JDBC storage.

My current opinion: this is a good foundation, but it is not production-ready yet. The main issue is not the overall design; it is that several production-facing behaviors are incomplete or can fail in real workloads.

## What Looks Good

- The module boundaries are clear:
  - `khrona-core` owns job definitions, triggers, scheduler behavior, retries, misfires, and the `JobStore` SPI.
  - `khrona-ktor` integrates scheduler lifecycle with Ktor application lifecycle.
  - `khrona-store-memory` provides a simple development/test store.
  - `khrona-store-jdbc` provides durable storage and database coordination.
- The Kotlin DSL is small and approachable for simple recurring, cron, and one-time jobs.
- The scheduler contains the right basic ingredients for distributed execution: persisted executions, claiming, lease expiry, heartbeat, deterministic IDs, lock keys, and misfire handling.
- The test coverage is strong for the project size, including Testcontainers coverage for Postgres, MySQL, Oracle, and H2.
- The repository has useful project documentation under `.specs`, which makes the project easier to continue.

## Main Risks And Problems

### 1. JDBC-backed jobs may execute no-op handlers

`JobDefinition.handler` is transient, so it is not serialized when jobs are saved to JDBC. `JdbcJobStore` deserializes `JobDefinition` from JSON, which recreates the job with the default empty handler. The scheduler later fetches the job from the store and calls that handler.

Relevant files:

- `khrona-core/src/main/kotlin/io/khrona/core/JobDefinition.kt`
- `khrona-store-jdbc/src/main/kotlin/io/khrona/store/jdbc/JdbcJobStore.kt`
- `khrona-core/src/main/kotlin/io/khrona/core/Scheduler.kt`

Impact: production JDBC mode can look like it is claiming and succeeding jobs while not actually running the user's code.

Potential direction:

- Keep runtime handlers in an in-memory registry keyed by job ID.
- Store only serializable metadata in JDBC.
- When executing, combine persisted execution state with the locally registered runtime handler.
- Add a test proving a JDBC-backed scheduler executes the configured handler after the job has been saved and reloaded.

### 2. Recurring jobs can stop after retry exhaustion

The scheduler schedules the next recurring execution only after a successful run. If a recurring execution fails and exhausts retries, it becomes `DEAD_LETTERED`, but the next normal scheduled occurrence is not created.

Relevant file:

- `khrona-core/src/main/kotlin/io/khrona/core/Scheduler.kt`

Impact: one bad run can permanently stop future runs of a recurring job.

Potential direction:

- Separate retry scheduling from recurring schedule advancement.
- After a terminal failure for one occurrence, schedule the next occurrence if the trigger is recurring.
- Add tests for recurring jobs that fail permanently but still schedule their next normal run.

### 3. `timeout` is defined but not enforced

`JobDefinition.timeout` exists in the public API, but the scheduler currently calls the handler directly without wrapping it in `withTimeout`.

Relevant files:

- `khrona-core/src/main/kotlin/io/khrona/core/JobDefinition.kt`
- `khrona-core/src/main/kotlin/io/khrona/core/Scheduler.kt`

Impact: users can configure a timeout that has no effect.

Potential direction:

- Apply `withTimeout` or `withTimeoutOrNull` around handler execution.
- Decide whether timeout should produce `FAILED`, retryable failure, or `DEAD_LETTERED`.
- Add tests for timed-out executions and retry behavior after timeout.

### 4. `ConcurrencyPolicy.REPLACE` exists but is not implemented

The enum includes `ALLOW`, `FORBID`, and `REPLACE`, but scheduler logic only has explicit behavior for `FORBID`.

Relevant files:

- `khrona-core/src/main/kotlin/io/khrona/core/JobDefinition.kt`
- `khrona-core/src/main/kotlin/io/khrona/core/Scheduler.kt`

Impact: users may configure `REPLACE` and assume it works, but it behaves effectively like neither a clearly documented replacement policy nor a complete concurrency strategy.

Potential direction:

- Either implement `REPLACE` fully or remove/hide it until supported.
- Define exact semantics:
  - cancel running execution and replace it,
  - mark previous execution as superseded,
  - or only replace pending executions.
- Add tests for all concurrency policies.

### 5. Public async methods hide failures

`registerJob` and `trigger` launch background coroutines and return immediately. Exceptions inside those coroutines are not naturally visible to callers.

Relevant file:

- `khrona-core/src/main/kotlin/io/khrona/core/Scheduler.kt`

Impact: caller code cannot reliably know whether registration or manual trigger succeeded.

Potential direction:

- Make these APIs `suspend fun`.
- Or return a `Job`, `Deferred`, or explicit result type.
- Add tests showing callers can observe failure when triggering an unknown job.

### 6. JDBC payload persistence is lossy

JDBC saves payloads using `payload?.toString()`. This loses type and structure.

Relevant file:

- `khrona-store-jdbc/src/main/kotlin/io/khrona/store/jdbc/JdbcJobStore.kt`

Impact: typed payloads do not survive persistence as structured data.

Potential direction:

- Define payload as JSON explicitly, probably `JsonElement?` or `String?` containing JSON.
- Provide typed helper APIs on top of the core.
- Add round-trip tests for structured payloads.

### 7. Database schema and indexing are minimal

The schema has a primary key and one index on `(status, scheduled_at)`. Lock checks and lease recovery will likely need better indexing as execution volume grows.

Relevant file:

- `khrona-store-jdbc/src/main/resources/khrona_schema.sql`

Impact: large installations may see slow polling, lock checks, and stale execution recovery.

Potential direction:

- Add indexes around:
  - `(lock_key, status, expires_at)`
  - `(status, expires_at)`
  - possibly `(job_id, scheduled_at)`
- Recheck query plans per supported dialect.

## Recommended Priority

1. Fix the JDBC handler/runtime registry model.
2. Fix recurring schedule advancement after failed/dead-lettered executions.
3. Either implement or remove unsupported public features: `timeout` and `REPLACE`.
4. Make `registerJob` and `trigger` observable, preferably suspend APIs.
5. Define payload serialization explicitly.
6. Improve JDBC indexes and migration strategy.

## Verification From Review

Command run:

```bash
./gradlew clean test
```

Result:

```text
BUILD SUCCESSFUL
22 actionable tasks: 21 executed, 1 up-to-date
```

The first attempt failed because the sandbox could not write to the local Gradle cache under `~/.gradle`; rerunning with filesystem approval completed successfully.

## Follow-Up Work Ideas

- Add a focused failing test for JDBC-backed handler execution.
- Add a failing test showing recurring jobs continue after retry exhaustion.
- Create a small ADR for the handler registry design before changing the storage model.
- Split the scheduler execution path into smaller internal methods after behavior is covered by tests.

## Resolution Notes

Updated after v0.3.1 hardening work:

- JDBC handler loss was addressed with an in-memory `HandlerRegistry`.
- Recurring schedules now continue after terminal execution failure.
- `timeout` is enforced around handler execution.
- `registerJob` and `trigger` are suspend functions.
- `REPLACE` now claims the replacement before superseding older active executions.
- JDBC payloads preserve JSON-compatible maps, lists, strings, numbers, booleans, and nested combinations.
- JDBC migration now fails fast on real schema errors while preserving idempotency for known create-index/table-exists cases.
