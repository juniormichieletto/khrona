# Job Management Core API Tasks

## Task 1: Store Query Contract

- [ ] Add failing `JobStoreContract` tests for `listExecutions(query)`.
- [ ] Cover job ID filtering, status filtering, scheduled time range filtering, bounded limit, and newest-first ordering.
- [ ] Implement query support in Memory, JDBC, Redis, and test stores.
- [ ] Verify with store-focused tests before moving to scheduler behavior.

## Task 2: Status And Management Models

- [ ] Add management DTOs in `khrona-core`.
- [ ] Add `CANCELLED` terminal status.
- [ ] Ensure Memory, JDBC, Redis, and mocks treat `CANCELLED` as terminal for completion timestamps and index cleanup.
- [ ] Add serialization/backward-compatibility coverage for new `JobDefinition.paused`.
- [ ] Define the handler execution context API shape, including a shutdown checkpoint function.

## Task 3: Pause And Resume

- [ ] Add failing scheduler tests proving paused jobs do not execute pending work.
- [ ] Add `paused: Boolean = false` to `JobDefinition` and DSL support.
- [ ] Implement `pauseJob` and `resumeJob`.
- [ ] Re-read job definitions before claiming and skip paused jobs before claim/execution.
- [ ] Add a resume test proving existing pending work can execute after resume through normal misfire handling.

## Task 4: Management Facade

- [ ] Add failing tests for `listJobOverviews`, `getJobOverview`, `listJobHistory`, and `startJob`.
- [ ] Implement status-derived progress aggregation.
- [ ] Keep `trigger` behavior intact and expose `startJob` as a clearer management alias.
- [ ] Include local handler availability in job overview by checking the scheduler's handler registry.

## Task 5: Local Stop

- [ ] Add failing tests for stopping an active local execution.
- [ ] Implement `stopExecution` and `stopJob`.
- [ ] Persist `CANCELLED` for successfully stopped local executions.
- [ ] Return a clear non-success result for non-local or unknown executions.
- [ ] Document that cross-node stop requires a later command-channel design.

## Task 6: Shutdown Checkpoint

- [ ] Add failing scheduler tests proving a handler calling the checkpoint stops during graceful shutdown before executing later handler code.
- [ ] Add a test proving checkpoint interruption does not mark the execution `SUCCESS` or terminal `CANCELLED`.
- [ ] Add a durable-store-oriented test proving checkpoint interruption releases or persists the execution so it can run again after restart without waiting for lease expiry.
- [ ] Implement scheduler stopping-state propagation to the checkpoint API.
- [ ] Document the distinction between shutdown checkpoints, coroutine cancellation checks such as `ensureActive()`, and application-owned progress persistence.

## Task 7: Documentation And Verification

- [ ] Update README examples when the API is implemented.
- [ ] Keep planned core API docs clearly marked as planned until code exists.
- [ ] Verify docs do not promise Khrona-owned admin routes, REST endpoints, dashboards, or UI.
- [ ] Run focused module tests during development.
- [ ] Run the required full suite:

```bash
./gradlew clean test
```

## Implementation Order

1. Store query contract and built-in store implementations.
2. DTOs, terminal status handling, and paused job model.
3. Scheduler pause/resume behavior.
4. Overview/history/start facade.
5. Local stop behavior.
6. Shutdown checkpoint behavior.
7. README docs, full test suite, and final review.
