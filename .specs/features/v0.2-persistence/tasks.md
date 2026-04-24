# Tasks: v0.2 - Persistence & Reliability

## Phase 1: Core Enhancements
- [x] **Task 1.1:** Add `RetryPolicy` and update `JobDefinition`. `[REQ-2.3]`
- [x] **Task 1.2:** Update `JobExecution` to include `retryCount` and `lastError`. `[REQ-2.3]`
- [x] **Task 1.3:** Update `Scheduler` to handle retries and next execution calculation. `[REQ-2.3, REQ-2.4]`

## Phase 2: JDBC Module & Schema
- [x] **Task 2.1:** Create `khrona-store-jdbc` module. `[REQ-2.1]`
- [x] **Task 2.2:** Define database schema and migration script (or DDL). `[REQ-2.1]`

## Phase 3: JDBC Implementation
- [x] **Task 3.1:** Implement `JdbcJobStore` (basic CRUD). `[REQ-2.1]`
- [x] **Task 3.2:** Implement `claimExecution` with `FOR UPDATE SKIP LOCKED`. `[REQ-2.1]`
- [x] **Task 3.3:** Add support for external transactions (basic). `[REQ-2.2]`

## Phase 4: Integration & Testing
- [x] **Task 4.1:** Setup H2/PostgreSQL for testing in `khrona-store-jdbc`.
- [x] **Task 4.2:** Integration test for durable scheduling and restart survival.
- [x] **Task 4.3:** Integration test for multi-worker claiming.
