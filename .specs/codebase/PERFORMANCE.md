# PERFORMANCE — Khrona

This guide explains the runtime and database load Khrona creates, how to size the main scheduler settings, and what to watch in production.

## Scheduler Load Model

Khrona currently uses a portable fixed polling loop. Each scheduler instance wakes every `pollingInterval`, asks the store for eligible executions, processes up to `pollBatchSize`, then sleeps again.

The rough idle polling cost is:

```text
database eligibility queries per second = scheduler instances / pollingIntervalSeconds
```

Examples:

- 1 instance with `pollingInterval = 1s`: about 1 eligibility query per second.
- 5 instances with `pollingInterval = 1s`: about 5 eligibility queries per second.
- 10 instances with `pollingInterval = 100ms`: about 100 eligibility queries per second.

The default `pollingInterval` is 1 second. This is reasonable for many applications, but high instance counts or very low intervals can create noticeable database traffic.

## Database Operations

With `JdbcJobStore`, Khrona borrows a connection from the configured `DataSource` for each store operation and returns it immediately after the operation. With a production pool such as HikariCP, this means pooled connection checkout and return, not a new TCP connection for every query.

Typical operations:

- Startup and registration save job definitions and create the first scheduled execution when needed.
- Each poll runs a bounded `listEligibleExecutions(now, pollBatchSize)` query.
- About once per minute, each scheduler instance calls `resetExpiredExecutions(now)` to recover stale claimed or running work.
- Each eligible execution may read its job definition, check lock state, claim the execution, and update status as it runs.
- Recurring jobs insert the next scheduled execution after a terminal result.
- Failed jobs may insert retry executions or update dead-letter state.
- Long-running jobs heartbeat periodically while they are `CLAIMED` or `RUNNING`.

The database load is normally small when there are few scheduler instances and moderate polling intervals. The biggest multipliers are scheduler instance count, short polling intervals, large backlogs, frequent retries, and many long-running jobs heartbeating at once.

## Configuration Tuning

- `pollingInterval`: Controls how often each scheduler instance checks for eligible work. Increase it to reduce idle database traffic. Decrease it when jobs must start with lower latency.
- `pollBatchSize`: Limits how many eligible executions a scheduler processes per poll. Increase it for large backlogs. Keep it bounded so one poll does not load or claim too much work at once.
- `executionLeaseDuration`: Controls how long claimed work remains owned without heartbeat renewal. It should be longer than expected transient pauses, database latency spikes, and normal heartbeat delay.
- `heartbeatInterval`: Defaults to half of `executionLeaseDuration`. Shorter heartbeats detect lost workers sooner but create more writes. Longer heartbeats reduce writes but require a longer lease and slower recovery.
- Job frequency: Khrona validates that interval and cron frequency are not smaller than `pollingInterval`. Use a polling interval that matches the minimum schedule resolution you need.

Practical defaults for many server applications:

- Keep `pollingInterval` between 1 and 5 seconds unless near-immediate pickup is required.
- Keep `pollBatchSize` high enough for expected bursts but low enough to avoid large single-poll spikes.
- Keep `heartbeatInterval` comfortably below `executionLeaseDuration`.
- Use a database connection pool sized for application traffic plus scheduler activity.

## Store Differences

`MemoryJobStore` has very low overhead and is useful for development, tests, and ephemeral jobs. It does not preserve execution state across process restart and does not coordinate multiple application instances.

`JdbcJobStore` adds database I/O so jobs and executions survive restart and can be coordinated across nodes. It is the right choice for production durability and distributed execution, but the application should account for polling reads, status writes, claim updates, and heartbeat writes.

## Multi-Instance Impact

Every scheduler instance polls independently. If five application instances run Khrona against the same database, the idle polling query rate is roughly five times a single instance.

Distributed mode prevents the same execution from being claimed by multiple workers, but it does not remove the cost of each node checking for work. For large deployments, tune `pollingInterval`, `pollBatchSize`, database pool size, and scheduler instance count together.

## Job Handler Guidance

Khrona's own scheduler operations are usually small compared with user job handlers. A handler that performs large queries, long transactions, unbounded loops, or high-volume API calls can dominate the system cost.

Recommended handler practices:

- Keep database transactions short.
- Make handlers idempotent, especially for durable stores and long-running work.
- Checkpoint long workflows outside Khrona if they need resumable progress.
- Split very large work into smaller executions where practical.
- Avoid launching unbounded parallel work from a single handler.
- Use job-specific rate limits or external throttling for expensive downstream systems.

## Operational Recommendations

Monitor these signals in production:

- Scheduler database query rate and latency.
- Connection pool usage and wait time.
- Count of pending, claimed, running, failed, dead-lettered, and misfired executions.
- Execution backlog age: oldest pending `scheduledAt` compared with current time.
- Heartbeat failures and expired execution recovery count.
- Job runtime distribution and retry volume.

Tune based on the bottleneck:

- If the database is idle but jobs start too slowly, lower `pollingInterval`.
- If the database sees too many idle queries, raise `pollingInterval`.
- If backlog drains too slowly, raise `pollBatchSize` or add scheduler instances.
- If the database is saturated, reduce scheduler instance count, raise `pollingInterval`, lower handler concurrency, or optimize handler database work.
- If long jobs are reclaimed while still running, increase `executionLeaseDuration` or shorten `heartbeatInterval`.

## Future Optimization

The planned cross-database adaptive scheduler delay would reduce idle polling by sleeping until the next known execution, stale recovery deadline, or bounded fallback interval. Database-native wake-up mechanisms such as PostgreSQL notifications may be added later as optional optimizations, but the portable baseline should continue working across Memory, H2, PostgreSQL, MySQL, and Oracle.
