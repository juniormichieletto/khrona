# Design: v0.7 - Android SQLite Store

## Problem

JDBC is a poor fit for Android. Android apps need local persistence through SQLite APIs that work with the Android runtime, packaging model, and lifecycle. Khrona's existing core abstractions are close to what Android needs, but the storage implementation must avoid server-oriented assumptions.

The Android store should make scheduled work durable. It should not pretend to bypass Android background execution limits. If the app process is stopped and Android does not allow it to run, Khrona cannot execute jobs until the host app is started by the OS or by an app-level scheduler such as WorkManager.

## Module Boundary

Add a new module:

```text
khrona-store-android-sqlite
```

The module should depend on:

- `khrona-core`
- Android SQLite APIs or AndroidX SQLite wrappers selected during implementation
- Kotlin coroutines support already used by Khrona

The module must not be a transitive requirement for `khrona-core`, `khrona-store-memory`, `khrona-store-jdbc`, or `khrona-ktor`.

## Public API Shape

The Android module should expose a small store factory that accepts an app-owned database configuration. The exact type should be selected during implementation, but the API should keep these properties:

- The app controls database name and location.
- Khrona controls schema objects inside the database.
- The store can run inside normal Android app processes.
- The store is closeable or lifecycle-aware enough to release SQLite resources.
- The API does not require consumers to understand internal Khrona tables.

Candidate direction:

```kotlin
class AndroidSQLiteJobStore(
    ...
) : JobStore
```

The constructor should prefer stable Android database abstractions over direct file handling. If Room is considered, it should be treated as an internal implementation detail unless exposing it materially improves consumer ergonomics.

## Schema

Use tables equivalent to the JDBC store concepts:

- job definitions and serialized trigger/handler metadata
- executions with status, scheduled time, attempt count, timestamps, and worker ownership
- locks for concurrency policies
- heartbeat and lease metadata
- retry/dead-letter metadata
- payload storage compatible with Khrona's structured payload behavior

SQLite constraints and indexes should support:

- eligible execution lookup by status and scheduled time
- bounded batch claiming
- stale claimed/running recovery lookup
- lock lookup by key
- dead-letter lookup by job or execution id

## Transactions and Concurrency

SQLite has different concurrency characteristics than server databases. The store should keep write transactions short and explicit.

Critical operations requiring transactions:

- enqueue execution
- claim execution
- update execution status
- heartbeat update
- mark stale execution as retryable or failed
- acquire/release lock
- save recurring next execution

The implementation should avoid long-running transactions around handler execution. Handler execution remains outside the store transaction, as in the existing scheduler model.

## Lifecycle and Background Execution

The Android store persists scheduler state, but it does not wake the app by itself.

Document the supported contract:

- When the app process is running and the scheduler is started, Khrona can claim and execute due jobs.
- If the app process dies, pending work remains in SQLite.
- On app restart, Khrona can recover stale executions and resume scheduling according to the existing at-least-once model.
- If the app needs background execution, the host app should use WorkManager, foreground services, alarms, or platform-appropriate entry points to start the scheduler when Android permits execution.

The first implementation should provide guidance rather than hard-coupling Khrona to WorkManager.

## Adaptive Delay Interaction

Android SQLite should implement any store capabilities required by the adaptive scheduler plan, including earliest pending execution lookup when that feature is implemented.

If Android SQLite ships before adaptive delay, its task list should include follow-up compatibility tests once adaptive delay lands.

## Testing Strategy

Reuse shared `JobStore` contract tests where possible. Android-specific testing should cover:

- schema creation
- schema migration
- process-restart simulation by closing and reopening the database
- transaction rollback behavior
- claim contention behavior
- stale execution recovery
- payload serialization compatibility

Implementation should choose the least fragile test setup that works with the Gradle structure. Options include Android instrumented tests, Robolectric tests, or a split where shared SQL behavior is tested on JVM and Android integration is tested separately.

Phase 1 implementation starts by extracting portable `JobStore` contract tests into `khrona-core` test fixtures. Store modules should consume this fixture so Memory, JDBC, and Android SQLite prove the same baseline behavior before adding backend-specific coverage.

The Android module must not be added to `settings.gradle.kts` until the Android build stack is selected and available in CI/local development. The current repo root applies JVM conventions to all subprojects, so introducing an Android library module also requires splitting the root Gradle convention so Android subprojects are not configured as plain JVM libraries.

## Expected Tradeoffs

- Android support expands Khrona's use cases but introduces a new build target and test environment.
- Keeping WorkManager out of core preserves separation, but users need clear docs to wire OS-level background execution.
- SQLite is durable and local, but it does not provide the same multi-node semantics as server databases.
- The module should optimize for single-device app-local scheduling, not distributed coordination.
