# Spec: v0.4 - Cross-Database Adaptive Delay Scheduler

## Overview

Khrona currently uses fixed polling: every scheduler node wakes at `pollingInterval`, asks the store for eligible executions, and then sleeps again. This is simple and portable, but it creates unnecessary database traffic and coroutine wakeups when no work is due.

This feature will introduce a cross-database adaptive scheduler loop that sleeps until useful work is expected, while retaining bounded fallback polling so external work created by other nodes is still discovered without database-specific notification features.

## Objectives

- Reduce idle scheduler database queries and CPU wakeups.
- Preserve bounded discovery latency for work created by other nodes.
- Keep the baseline implementation portable across `MemoryJobStore`, H2, PostgreSQL, MySQL, and Oracle.
- Preserve stale execution recovery timing.
- Wake immediately for work created by the local scheduler instance.
- Keep fixed polling available as a compatibility mode or fallback.

## Requirements

- **REQ-A1: Cross-Database Baseline:** The adaptive scheduler must work with all implemented stores without requiring Postgres `NOTIFY`, MySQL events, Oracle AQ, database triggers, or external queues.
- **REQ-A2: Next Pending Time:** `JobStore` must expose a capability to return the earliest pending execution scheduled after a supplied `now`.
- **REQ-A3: Hybrid Sleep:** The scheduler must sleep until the earliest of next pending execution time, next stale recovery deadline, or configured max polling interval.
- **REQ-A4: Bounded External Discovery:** Work inserted by another node must be discovered no later than the configured max polling interval.
- **REQ-A5: Local Wake-Up:** Local `registerJob`, `trigger`, retry scheduling, and next recurring scheduling must wake the scheduler loop immediately after saving new work.
- **REQ-A6: Recovery Preservation:** Stale execution recovery must continue to run on its configured cadence even when no jobs are due.
- **REQ-A7: Compatibility Mode:** Fixed polling must remain available until adaptive delay is stable and proven across stores.

## Non-Goals

- Exact immediate cross-node wake-up.
- Database-specific notification implementations.
- External queue integration.
- Replacing the existing lease/claiming model.

## Success Criteria

- Idle schedulers perform fewer store queries than fixed 1-second polling.
- Known future executions run within scheduler timing tolerance.
- Local manual triggers do not wait for the max polling interval.
- Cross-node inserted work is discovered within max polling interval.
- Existing distributed locking, misfire, retry, timeout, and recurring scheduling behavior remains intact.
