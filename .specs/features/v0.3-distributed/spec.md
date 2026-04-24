# Specification: v0.3 - Distributed & Coordination

## Overview
This feature enables Khrona to operate reliably in a multi-node environment. It introduces a lease-based claiming model, distributed locking policies, and mechanisms to recover from worker failures.

## Requirements

### REQ-3.1: Lease-Based Claiming
- **Description:** Job claiming must include a lease expiration time.
- **Details:**
    - `JobStore.claimExecution` must accept a `leaseDuration`.
    - The database schema must be updated to include `expires_at` (or `lease_expires_at`).
    - A claim is successful if the job is `PENDING` OR if it is `CLAIMED`/`RUNNING` but its lease has expired.
    - Workers should periodically "heartbeat" to extend the lease if the job is still running (optional for now, but recommended).

### REQ-3.2: Distributed Locking (FORBID)
- **Description:** Support for preventing concurrent execution of jobs sharing the same `lockKey`.
- **Details:**
    - If a `JobDefinition` has a `lockKey`, the scheduler must ensure only one execution with that key is `CLAIMED` or `RUNNING` across all nodes.
    - The `ConcurrencyPolicy` (e.g., `FORBID`, `REPLACE`) should be introduced. `FORBID` is the primary target for v0.3.
    - `FORBID`: If a lock is held, the new execution remains `PENDING` or is skipped until the lock is released.

### REQ-3.3: Stale Worker Recovery
- **Description:** Automatically recover jobs abandoned by crashed workers.
- **Details:**
    - A background process (or logic within the poll loop) should identify executions that have an expired lease.
    - Expired executions should be reset to `PENDING` (to be retried) or moved to `FAILED` if they have exceeded some threshold.

### REQ-3.4: Worker Liveness
- **Description:** Track active workers.
- **Details:**
    - (Optional) A `khrona_workers` table to track worker IDs and their last heartbeat.

## Success Criteria
- [ ] Multiple scheduler instances can run against the same database without double-executing the same `JobExecution`.
- [ ] If a worker node crashes mid-job, another node eventually picks up the job after the lease expires.
- [ ] Jobs with the same `lockKey` and `FORBID` policy do not run concurrently.
- [ ] Schema migration supports `expires_at`.
