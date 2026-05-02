# Tasks: v0.3.1 - Reliability Hardening

## Phase 1: Core Reliability
- [x] **Task 1.1:** Implement `HandlerRegistry` to store job handlers in-memory.
- [x] **Task 1.2:** Refactor `Scheduler` to use `HandlerRegistry` when executing claimed jobs.
- [x] **Task 1.3:** Update `Scheduler` to schedule the next occurrence of recurring jobs even after `DEAD_LETTERED` status.
- [x] **Task 1.4:** Wrap job handler execution in `withTimeout` using `JobDefinition.timeout`.

## Phase 2: API & Concurrency
- [x] **Task 2.1:** Change `registerJob` and `trigger` to `suspend fun` and handle storage errors.
- [x] **Task 2.2:** Implement `ConcurrencyPolicy.REPLACE` logic (cancel existing execution if possible).
- [x] **Task 2.3:** Add unit tests for `REPLACE` and `FORBID` concurrency policies.

## Phase 3: JDBC Improvements
- [x] **Task 3.1:** Update `JdbcJobStore` to use `kotlinx.serialization` for JSON payloads instead of `toString()`.
- [x] **Task 3.2:** Add indexes to `khrona_schema.sql` for `(lock_key, status, expires_at)` and `(status, expires_at)`.
- [x] **Task 3.3:** Create a comprehensive integration test proving JDBC round-trip with handlers and structured payloads.

## Phase 4: Review Follow-Up Fixes
- [x] **Task 4.1:** Restore fail-fast migration behavior while keeping duplicate-index idempotency.
- [x] **Task 4.2:** Ensure `REPLACE` claims the replacement before superseding existing executions.
- [x] **Task 4.3:** Make supersede updates exclude the replacement execution and return targeted execution IDs consistently.
- [x] **Task 4.4:** Fix JSON string payload escaping for JDBC payload storage.

## Verification
- [x] Failing test for JDBC handler loss is fixed.
- [x] Failing test for recurring job stoppage is fixed.
- [x] Timeout verification tests pass.
- [x] JSON payload round-trip tests pass.
- [x] Grand Finale E2E test passes across restarts.
- [x] Migration failures throw instead of being swallowed.
- [x] `REPLACE` does not supersede an existing execution when replacement claim fails.
- [x] JDBC string payloads with quotes and newlines round-trip correctly.
