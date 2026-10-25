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

## Fencing guarantees in 0.6.0

1. **Fenced status transitions and subsequent scheduling.** `updateExecutionStatus(id, status, error, workerId): Boolean` atomically validates that the execution is currently active (`CLAIMED` or `RUNNING`) and owned by `workerId` when `workerId` is supplied. A superseded claim cannot transition back to `RUNNING`, and a stale handler finishing after reclamation cannot overwrite the new owner's state with `SUCCESS`/`FAILED`/`DEAD_LETTERED` or schedule a duplicate retry or next recurring execution.
2. **Per-claim tokens and claimed-before-active shutdown safety.** The scheduler generates a unique per-claim token (`<workerId>:<claimId>`) for each claim attempt, skips executions already in `activeJobs`, registers a placeholder job immediately after `claimExecution` succeeds, and releases the execution back to `PENDING` (fenced by the per-claim token) if shutdown interrupts post-claim setup or cancels active jobs on timeout.

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
