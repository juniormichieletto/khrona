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
`JobStore.heartbeat(executionId: UUID, leaseDuration: Duration)`

The `Scheduler` will launch a background coroutine for each running job to heartbeat.

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
    - Add `heartbeat(id, duration)`
    - Add `isLockHeld(lockKey): Boolean`
    - Add `resetExpiredExecutions(): Int`

### khrona-store-jdbc
- Update `JdbcJobStore` and `JdbcDialect` implementations.
- Update `khrona_schema.sql` and `migrate()` logic.

### khrona-store-memory
- Update `MemoryJobStore` to support new methods and lease logic.

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
