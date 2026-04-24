# Design: v0.2 - Persistence & Reliability

## Database Schema (PostgreSQL)

### `khrona_jobs`
Stores job definitions.
- `id`: VARCHAR(255) PRIMARY KEY
- `definition_json`: TEXT (Serialized DSL/config)
- `created_at`: TIMESTAMP

### `khrona_executions`
Stores individual job executions.
- `id`: UUID PRIMARY KEY
- `job_id`: VARCHAR(255) REFERENCES khrona_jobs(id)
- `status`: VARCHAR(50) (PENDING, CLAIMED, RUNNING, SUCCESS, FAILED, DEAD_LETTER)
- `scheduled_at`: TIMESTAMP
- `claimed_at`: TIMESTAMP
- `claimed_by`: VARCHAR(255) (Worker ID)
- `payload_json`: TEXT
- `retry_count`: INT DEFAULT 0
- `last_error`: TEXT
- `created_at`: TIMESTAMP

## Claiming Logic
We will use `SELECT ... FOR UPDATE SKIP LOCKED` for efficient multi-worker claiming.

```sql
UPDATE khrona_executions
SET status = 'CLAIMED', claimed_at = ?, claimed_by = ?
WHERE id = (
    SELECT id 
    FROM khrona_executions 
    WHERE status = 'PENDING' AND scheduled_at <= ?
    ORDER BY scheduled_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING *;
```

## Retry Strategy
A new `RetryPolicy` class in `khrona-core`:
```kotlin
data class RetryPolicy(
    val maxRetries: Int = 3,
    val initialDelay: Duration = Duration.ofSeconds(1),
    val backoffMultiplier: Double = 2.0,
    val jitter: Double = 0.1
)
```

The `Scheduler` will be updated to calculate the next retry time based on this policy when a job fails.

## Transactional Enqueue
Provide an extension or a specific method that accepts a JDBC `Connection`.
```kotlin
suspend fun Khrona.enqueue(jobId: String, payload: Any?, connection: Connection)
```
This requires `JobStore` to have a way to participate in an external transaction.
Alternatively, for the first version, we can just ensure that `saveExecution` can be run within a user-provided transaction context if they use the same `DataSource`.

## Components
- `khrona-store-jdbc`: New module.
- `JdbcJobStore`: Implementation of `JobStore`.
- `PostgresDialect`: SQL dialect for PostgreSQL specifics.
