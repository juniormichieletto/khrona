# Design: v0.3 - Distributed & Coordination

## Architecture Changes

### 1. Lease Model
We will update `khrona_executions` table to include `expires_at`.
When claiming an execution, `expires_at` will be set to `now + leaseDuration`.

```sql
ALTER TABLE khrona_executions ADD COLUMN expires_at TIMESTAMP NULL;
```

The claiming SQL for Postgres will change to:
```sql
UPDATE khrona_executions
SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?, expires_at = ?
WHERE id = (
    SELECT id FROM khrona_executions
    WHERE id = ? 
    AND (status = 'PENDING' OR (status IN ('CLAIMED', 'RUNNING') AND expires_at < ?))
    FOR UPDATE SKIP LOCKED
)
```

### 2. Distributed Locking (FORBID)
We will introduce `ConcurrencyPolicy` to `JobDefinition`.

```kotlin
enum class ConcurrencyPolicy {
    ALLOW,  // Multiple executions allowed (default)
    FORBID, // Don't start if another is running
    REPLACE // (Future) Cancel existing and start new
}
```

The `JobStore` will need a way to check for active locks.
`JobStore.isLockHeld(lockKey: String): Boolean`

In `Scheduler.pollAndExecute()`:
- Filter eligible executions by lock availability if policy is `FORBID`.

### 3. Heartbeating
To prevent jobs from being reclaimed while still running, workers should heartbeat.
As of the upcoming 0.6.0 release, the public SPI is:
`JobStore.heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean`

The `Scheduler` launches a background coroutine for each running job and passes its worker ID. Each store atomically validates active status (`CLAIMED`/`RUNNING`), matching ownership, and an unexpired lease before extending it. A failed check returns `false` without modifying the execution, and the scheduler requests cooperative cancellation of the handler.

An expired lease cannot be revived, even before reclamation. This changes the previous behavior: pauses or storage delays that exhaust the remaining lease can cancel healthy work. The default heartbeat interval is half the execution lease; choose a lease with enough margin for expected pauses. JDBC compares stored timestamps against the application clock, as claiming already does, so clock skew between workers remains relevant.

### 4. Stale Worker Recovery
A new method in `JobStore`:
`JobStore.resetExpiredExecutions(): Int`

This will find jobs with `expires_at < now` and status `CLAIMED` or `RUNNING`, and reset them to `PENDING`.
This could be integrated into the `Scheduler`'s main loop or a separate maintenance loop.

## Component Updates

### khrona-core
- `JobExecution`: Add `expiresAt`.
- `JobDefinition`: Add `concurrencyPolicy`.
- `JobStore`:
    - Use `heartbeat(id, workerId, leaseDuration): Boolean` (breaking change in 0.6.0)
    - Add `isLockHeld(lockKey): Boolean`
    - Add `resetExpiredExecutions(): Int`

### khrona-store-jdbc
- Update `JdbcJobStore` and `JdbcDialect` implementations.
- Update `khrona_schema.sql` and `migrate()` logic.

### khrona-store-memory
- Update `MemoryJobStore` to support new methods and lease logic.

## Remaining fencing work

These are follow-ups to heartbeat ownership validation, not guarantees supplied by the current implementation:

1. **Fence final status writes and subsequent scheduling.** `updateExecutionStatus` does not validate ownership. A previous handler can finish between heartbeats or ignore cancellation, overwrite the new owner's state with `SUCCESS`/`FAILED`, and schedule a duplicate next execution. Require an atomic ownership/claim check for completion and failure transitions, and schedule the next run only when that transition succeeds. Regression tests must let an old handler finish after reclamation and verify that neither the new state nor next-run scheduling is changed by it.
2. **Distinguish successive claims by the same worker.** Polling does not skip IDs already in `activeJobs`; one scheduler can reclaim an expired execution whose original coroutine is still running, replacing the map entry. Both attempts have the same `workerId`, so worker-level validation cannot distinguish them. Introduce a per-claim UUID or monotonically increasing version, validate it on renewal and status transitions, and make local active-job tracking safe for overlapping attempts. Regression tests must reclaim with the same worker ID and prove that the previous claim cannot renew, complete, or remove the newer attempt from tracking.

## Data Model Changes

### JobExecution
```kotlin
data class JobExecution(
    // ...
    val expiresAt: Instant? = null
)
```

### JobDefinition
```kotlin
data class JobDefinition(
    // ...
    val concurrencyPolicy: ConcurrencyPolicy = ConcurrencyPolicy.ALLOW
)
```
