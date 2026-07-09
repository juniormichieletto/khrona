# Job Management Core API Design

## Public API Shape

Add core management models in `khrona-core`:

- `JobExecutionQuery`
- `JobOverview`
- `JobProgress`
- `JobControlResult`

Suggested fields:

- `JobExecutionQuery`: `jobId`, `statuses`, `limit`, `scheduledFrom`, `scheduledUntil`.
- `JobOverview`: `job`, `paused`, `hasLocalHandler`, `progress`.
- `JobProgress`: `latestExecution`, `nextPendingExecution`, `activeExecutions`, `countsByStatus`.
- `JobControlResult`: success flag, affected execution IDs, and a short reason for no-op or failure cases.

Add scheduler management functions that host applications can wrap in their own operational surfaces:

- `listJobOverviews()`
- `getJobOverview(jobId)`
- `listJobHistory(jobId, limit = 100)`
- `startJob(jobId, payload = null)`
- `pauseJob(jobId)`
- `resumeJob(jobId)`
- `stopExecution(executionId)`
- `stopJob(jobId)`

`startJob` should use the existing manual trigger semantics. Keep `trigger` intact for compatibility and expose `startJob` as the clearer management-oriented name.

## Storage Contract

Extend `JobStore` with:

```kotlin
suspend fun listExecutions(query: JobExecutionQuery = JobExecutionQuery()): List<JobExecution>
```

Built-in stores implement matching behavior:

- Memory filters and sorts in memory.
- JDBC queries `khrona_executions` by job, status, and scheduled time range, ordered newest first and bounded by `limit`.
- Redis reads stored executions from the persisted execution hash and filters/sorts in process for the portable baseline.

Default ordering is newest scheduled execution first. `limit` must be positive and should default to `100`.

## Pause Semantics

Add `paused: Boolean = false` to `JobDefinition`, plus DSL support for defining a job as initially paused.

Pause is persisted by saving the job definition with `paused = true`. Scheduler polling must re-read the job definition before claiming work and skip paused jobs before claim/execution. This preserves pending executions and history while preventing new work from starting.

Resume persists `paused = false`. Existing pending executions become eligible again through the normal scheduler path, including the existing misfire policy behavior.

## Progress Semantics

Progress is derived from execution state only:

- latest execution
- next pending execution
- active `CLAIMED` or `RUNNING` executions
- counts by `ExecutionStatus`
- timestamps, attempt, worker, and error fields

Manual handler progress reporting is intentionally deferred. Applications that need detailed percent/checkpoint reporting should keep that state in application-owned storage and link it to Khrona execution IDs.

## Stop Semantics

Add `CANCELLED` as a terminal `ExecutionStatus`.

`stopExecution` cancels only a coroutine tracked in the current scheduler instance. If cancellation succeeds, persist `CANCELLED`. If the execution is unknown or active on another node, return a non-success `JobControlResult` with a clear reason. Do not claim that remote work was stopped.

`stopJob(jobId)` applies `stopExecution` to all locally active executions for that job.

## Compatibility Notes

- Existing job definitions deserialize with `paused = false`.
- Existing store schemas do not need a new column if `paused` remains inside serialized `JobDefinition`.
- JDBC execution query can use existing `khrona_executions` columns and indexes.
- Redis does not need a new index for the first pass because the portable baseline can scan the stored execution hash.
- Khrona must not add Ktor routes, REST endpoint paths, dashboard components, or admin UI as part of this feature. Applications own those integration surfaces.
