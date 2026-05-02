# Tasks: v0.3.1 - Reliability Hardening

## Phase 1: Core Reliability
- [x] **Task 1.1:** Implement `HandlerRegistry` to store job handlers in-memory.
- [x] **Task 1.2:** Refactor `Scheduler` to use `HandlerRegistry` when executing claimed jobs.
- [ ] **Task 1.3:** Update `Scheduler` to schedule the next occurrence of recurring jobs even after `DEAD_LETTERED` status.
- [ ] **Task 1.4:** Wrap job handler execution in `withTimeout` using `JobDefinition.timeout`.

## Phase 2: API & Concurrency
- [ ] **Task 2.1:** Change `registerJob` and `trigger` to `suspend fun` and handle storage errors.
- [ ] **Task 2.2:** Implement `ConcurrencyPolicy.REPLACE` logic (cancel existing execution if possible).
- [ ] **Task 2.3:** Add unit tests for `REPLACE` and `FORBID` concurrency policies.

## Phase 3: JDBC Improvements
- [ ] **Task 3.1:** Update `JdbcJobStore` to use `kotlinx.serialization` for JSON payloads instead of `toString()`.
- [ ] **Task 3.2:** Add indexes to `khrona_schema.sql` for `(lock_key, status, expires_at)` and `(status, expires_at)`.
- [ ] **Task 3.3:** Create a comprehensive integration test proving JDBC round-trip with handlers and structured payloads.

## Verification
- Failing test for JDBC handler loss is fixed.
- Failing test for recurring job stoppage is fixed.
- Timeout verification tests.
- JSON payload round-trip tests.
