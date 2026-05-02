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
- On retrieval, keep it as a `String` (JSON) and provide a typed helper in `JobExecution` if possible, or leave it to the handler to decode. *Initial implementation will focus on preserving the JSON structure.*

## 6. JDBC Schema Optimization
**Problem:** Minimal indexing.
**Solution:**
- Add composite index on `(status, scheduled_at)` (already exists, but verify).
- Add composite index on `(lock_key, status, expires_at)` to speed up `isLockHeld` and `claimExecution` checks.
- Add index on `(status, expires_at)` for `resetExpiredExecutions`.
