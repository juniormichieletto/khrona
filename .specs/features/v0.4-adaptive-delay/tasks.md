# Tasks: v0.4 - Cross-Database Adaptive Delay Scheduler

## Phase 1: Store Capability

- [ ] **Task 1.1:** Add `JobStore.nextPendingExecutionTime(now: Instant): Instant?`.
- [ ] **Task 1.2:** Implement next pending execution lookup in `MemoryJobStore`.
- [ ] **Task 1.3:** Add portable JDBC dialect SQL for earliest pending execution time.
- [ ] **Task 1.4:** Implement JDBC support for H2, PostgreSQL, MySQL, and Oracle.

## Phase 2: Scheduler Loop

- [ ] **Task 2.1:** Add adaptive delay configuration while keeping fixed polling compatibility.
- [ ] **Task 2.2:** Implement adaptive sleep calculation using next pending execution, next recovery deadline, and max polling interval.
- [ ] **Task 2.3:** Add local wake signal to interrupt adaptive sleep.
- [ ] **Task 2.4:** Wake the scheduler after local `registerJob`, `trigger`, retry scheduling, recurring scheduling, and misfire rescheduling.
- [ ] **Task 2.5:** Update validation semantics so scheduler resolution is not incorrectly tied to fixed polling interval.

## Phase 3: Documentation

- [ ] **Task 3.1:** Document adaptive delay configuration and compatibility mode in README.
- [ ] **Task 3.2:** Update architecture docs to show adaptive sleep and bounded fallback polling.
- [ ] **Task 3.3:** Document that database-native wake-up mechanisms are optional future optimizations.

## Verification

- [ ] Idle scheduler does not poll every second when the next execution is far in the future.
- [ ] Local `trigger(...)` wakes an adaptive scheduler immediately.
- [ ] Local `registerJob(...)` wakes an adaptive scheduler immediately.
- [ ] Retry scheduling wakes an adaptive scheduler immediately.
- [ ] Recurring next-run scheduling wakes an adaptive scheduler immediately.
- [ ] Stale execution recovery runs on time even when no jobs are due.
- [ ] Memory, H2, PostgreSQL, MySQL, and Oracle return earliest pending execution time consistently.
- [ ] Multi-node external work is discovered within `maxPollingInterval` without database notifications.
- [ ] Existing fixed polling behavior remains available and tested.
