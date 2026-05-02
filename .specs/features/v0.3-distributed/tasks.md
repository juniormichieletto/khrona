# Tasks: v0.3 - Distributed & Coordination

## Task Group 1: Core Domain Updates
- [x] **Task 1.1:** Add `ConcurrencyPolicy` enum to `khrona-core`. [ID: T1.1]
- [x] **Task 1.2:** Update `JobDefinition` to include `concurrencyPolicy`. [ID: T1.2]
- [x] **Task 1.3:** Update `JobExecution` to include `expiresAt`. [ID: T1.3]
- [x] **Task 1.4:** Update `JobStore` interface with new methods: `heartbeat`, `isLockHeld`, `resetExpiredExecutions`. [ID: T1.4]

## Task Group 2: Memory Store Implementation
- [x] **Task 2.1:** Implement new `JobStore` methods in `MemoryJobStore`. [ID: T2.1]
- [x] **Task 2.2:** Update `MemoryJobStore.claimExecution` to support lease expiration. [ID: T2.2]
- [x] **Task 2.3:** Add unit tests for lease and locking in `MemoryJobStoreTest`. [ID: T2.3]

## Task Group 3: JDBC Store Implementation
- [x] **Task 3.1:** Update `khrona_schema.sql` to include `expires_at`. [ID: T3.1]
- [x] **Task 3.2:** Update `JdbcDialect` interface and implementations (Postgres, H2, MySql, Oracle) with new SQL. [ID: T3.2]
- [x] **Task 3.3:** Implement new `JobStore` methods in `JdbcJobStore`. [ID: T3.3]
- [x] **Task 3.4:** Update `JdbcJobStore.claimExecution` to use `expires_at`. [ID: T3.4]
- [x] **Task 3.5:** Update `JdbcJobStore.migrate()` to handle the new column (if needed for existing DBs, but for now we assume fresh schema or simple update). [ID: T3.5]

## Task Group 4: Scheduler Enhancements
- [x] **Task 4.1:** Update `Scheduler.pollAndExecute` to handle `FORBID` policy. [ID: T4.1]
- [x] **Task 4.2:** Implement heartbeating in `Scheduler` during job execution. [ID: T4.2]
- [x] **Task 4.3:** Add stale worker recovery loop (or call `resetExpiredExecutions` in poll loop). [ID: T4.3]
- [x] **Task 4.4:** Add integration tests for multi-node claiming and locking. [ID: T4.4]

## Verification Plan
- Unit tests for `MemoryJobStore` covering lease and lock.
- Integration tests using Testcontainers for `JdbcJobStore`.
- Multi-worker test simulation in `SchedulerTest`.
