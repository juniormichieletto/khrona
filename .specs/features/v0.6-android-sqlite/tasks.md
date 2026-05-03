# Tasks: v0.6 - Android SQLite Store

## Phase 1: Module and Test Harness

- [ ] **Task 1.1:** Add `khrona-store-android-sqlite` to Gradle settings with an Android-compatible library setup.
- [ ] **Task 1.2:** Choose and document the Android SQLite access layer after verifying compatibility with the current Gradle/Kotlin setup.
- [ ] **Task 1.3:** Extract or reuse shared `JobStore` contract tests so Memory, JDBC, and Android SQLite can be compared consistently.
- [ ] **Task 1.4:** Add an Android SQLite test harness that can create, close, reopen, and clean an isolated database.

## Phase 2: Schema and Migrations

- [ ] **Task 2.1:** Define versioned SQLite schema creation for jobs, executions, locks, heartbeat/lease fields, retry state, payloads, and dead-letter state.
- [ ] **Task 2.2:** Add indexes for eligible execution lookup, bounded claiming, stale recovery, lock lookup, and dead-letter inspection.
- [ ] **Task 2.3:** Implement explicit migration handling for future schema versions.
- [ ] **Task 2.4:** Add tests that verify schema creation and reopen behavior without losing pending executions.

## Phase 3: JobStore Implementation

- [ ] **Task 3.1:** Implement job definition persistence and lookup.
- [ ] **Task 3.2:** Implement execution enqueue, list/claim, status updates, and bounded polling behavior.
- [ ] **Task 3.3:** Implement heartbeat and stale execution recovery fields.
- [ ] **Task 3.4:** Implement lock acquisition/release semantics for concurrency policies.
- [ ] **Task 3.5:** Implement retry and dead-letter persistence.
- [ ] **Task 3.6:** Implement structured payload persistence without silent fallback behavior.
- [ ] **Task 3.7:** Ensure critical transitions use short SQLite transactions.

## Phase 4: Android Runtime Integration

- [ ] **Task 4.1:** Add lifecycle-safe close/release behavior for SQLite resources.
- [ ] **Task 4.2:** Simulate app process restart by closing and reopening the store while preserving scheduler state.
- [ ] **Task 4.3:** Document that OS wake-up remains the host app's responsibility.
- [ ] **Task 4.4:** Add WorkManager interop guidance or a minimal sample showing how an app can start Khrona when background execution is allowed.

## Phase 5: Compatibility and Documentation

- [ ] **Task 5.1:** Update README with Android SQLite setup and dependency guidance.
- [ ] **Task 5.2:** Update architecture docs to show Android SQLite as a store implementation.
- [ ] **Task 5.3:** Document behavior across app restart, process death, device sleep, reboot, and wall-clock changes.
- [ ] **Task 5.4:** Add compatibility notes for adaptive delay and earliest pending execution lookup.

## Verification

- [ ] Android SQLite store passes the shared `JobStore` contract tests.
- [ ] Pending executions survive database close/reopen.
- [ ] Claimed or running executions can be recovered after simulated process death.
- [ ] Retry and dead-letter state survives restart.
- [ ] Payload persistence matches existing structured payload behavior.
- [ ] Lock behavior matches existing store semantics for single-device scheduling.
- [ ] Memory and JDBC store tests remain green.
- [ ] Full suite passes with `./gradlew clean test`.
