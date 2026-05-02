# Design: v0.4 - Cross-Database Adaptive Delay Scheduler

## Problem

The scheduler currently sleeps for a fixed `pollingInterval` after each polling cycle. With a 1-second interval, every scheduler node queries the store every second even when the next execution is minutes or hours away.

Pure sleep-until-next is not safe in distributed mode. A node can calculate that its next known execution is far in the future, then another node can insert a manual trigger or retry that should run sooner. Without a wake-up mechanism, the sleeping node would not discover that work.

## Baseline Approach

Use hybrid adaptive polling:

1. Poll and execute currently eligible executions.
2. Run stale execution recovery when its deadline is due.
3. Ask the store for the next pending execution time after `now`.
4. Calculate delay as the minimum of:
   - time until next pending execution,
   - time until next stale recovery deadline,
   - configured max polling interval.
5. Suspend until that delay expires or a local wake signal arrives.

This preserves database portability because every store can support the same core query shape:

```sql
SELECT MIN(scheduled_at)
FROM khrona_executions
WHERE status = 'PENDING'
AND scheduled_at > ?
```

Memory storage should compute the same value from its in-memory execution map.

## Configuration Model

Add adaptive scheduler configuration without removing fixed polling:

- `adaptiveDelayEnabled: Boolean = false` initially.
- `maxPollingInterval: Duration` bounds cross-node discovery latency.
- `minPollingInterval: Duration` prevents tight loops when the next due time is immediate or clock precision is low.

`pollingInterval` can remain the fixed polling interval in compatibility mode. When adaptive delay is enabled, it may either become the default max interval or be superseded by `maxPollingInterval`.

## Local Wake-Up

The scheduler should have an internal coroutine signal, such as a conflated channel or shared signal, that wakes the loop after local work is saved.

Wake the loop after:

- `registerJob` schedules a first execution.
- `trigger` saves a manual execution.
- retry scheduling saves a retry execution.
- recurring scheduling saves the next execution.
- misfire handling saves a replacement next execution.

This only solves local wake-up. Cross-node wake-up remains bounded by `maxPollingInterval` in the portable baseline.

## Recovery Timing

Stale recovery currently runs roughly once per minute. Adaptive delay must include the next recovery deadline in its sleep calculation so an idle scheduler still recovers expired `CLAIMED` or `RUNNING` executions on time.

## Database Compatibility

The baseline must work across:

- `MemoryJobStore`
- H2
- PostgreSQL
- MySQL
- Oracle

Database-specific notifications are explicitly optional future optimizations. Postgres `LISTEN/NOTIFY`, MySQL events, Oracle AQ, triggers, external queues, and pub/sub systems must not be required for the portable version.

## Expected Tradeoffs

- Idle load should decrease substantially for sparse schedules.
- Cross-node manual work may still wait up to `maxPollingInterval`.
- The scheduler loop becomes more complex and needs stronger timing tests.
- Validation rules around job frequency should move away from “must be greater than `pollingInterval`” toward documented minimum scheduler resolution.
