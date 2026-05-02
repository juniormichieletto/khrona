# Design: v0.3.1 - Reliability Hardening

## 1. Handler Registry (Addressing JDBC Handler Loss)
**Problem:** `JobDefinition.handler` is transient and lost during JDBC serialization.
**Solution:** 
- Introduce a `HandlerRegistry` component within the `Scheduler`.
- When a job is registered via `registerJob` or configured via DSL, its handler is stored in the `HandlerRegistry` keyed by `jobId`.
- When the `Scheduler` claims an execution and fetches the `JobDefinition` from the `JobStore`, it looks up the handler in the `HandlerRegistry` before execution.

## 2. Resilient Recurring Schedules
**Problem:** Next recurring run is only scheduled if the current run succeeds or is retried. If terminal failure occurs, the chain breaks.
**Solution:**
- Refactor `Scheduler.executeJob` to ensure `scheduleNextRun()` is called in a `finally` block or specifically handled after terminal `DEAD_LETTERED` state.
- Ensure that the "retry" logic and "recurring" logic are decoupled.

## 3. Execution Timeouts
**Problem:** `JobDefinition.timeout` is ignored.
**Solution:**
- Use Kotlin's `withTimeout(duration)` around the call to `jobDef.handler(execution.payload)`.
- Catch `TimeoutCancellationException` and treat it as a `FAILED` execution (which can then be retried based on policy).

## 4. Observable API
**Problem:** `registerJob` and `trigger` are fire-and-forget.
**Solution:**
- Change `registerJob` and `trigger` to `suspend` functions.
- Await the completion of the `store.saveJob` and `store.saveExecution` calls before returning.
- Remove the internal `scope.launch` inside these methods unless explicitly requested for backgrounding.

## 5. Structured Payloads
**Problem:** `JdbcJobStore` uses `payload?.toString()`.
**Solution:**
- Use `kotlinx-serialization-json` to serialize payloads to a JSON string in `khrona_executions.payload_json`.
- On retrieval, convert JSON primitives, arrays, and objects back into Kotlin-compatible `String`, `Number`, `Boolean`, `List`, and `Map` structures.
- String payloads must be encoded through JSON primitives rather than manual quoting so quotes, backslashes, and newlines round-trip correctly.

## 6. JDBC Schema Optimization
**Problem:** Minimal indexing.
**Solution:**
- Add composite index on `(status, scheduled_at)` (already exists, but verify).
- Add composite index on `(lock_key, status, expires_at)` to speed up `isLockHeld` and `claimExecution` checks.
- Add index on `(status, expires_at)` for `resetExpiredExecutions`.

## 7. Review Follow-Up Hardening
**Problem:** Review found remaining edge cases in migration failure handling and `REPLACE` ordering.
**Solution:**
- Run JDBC migration in a transaction and throw on real migration failures while keeping known idempotent create-index/table-exists cases safe.
- For `REPLACE`, claim the replacement execution before superseding existing active executions.
- Exclude the claimed replacement execution from supersede updates.
- Use row locking during JDBC supersede selection so selected active executions match the rows being updated.
