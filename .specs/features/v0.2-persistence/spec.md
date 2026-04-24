# Specification: v0.2 - Persistence & Reliability

## Overview
This feature adds durable storage support using JDBC (targeting PostgreSQL) and introduces reliability features like retries and transactional enqueueing.

## Requirements

### REQ-2.1: JDBC Job Store
- **Description:** Implement `JobStore` using JDBC.
- **Details:**
    - Must support PostgreSQL.
    - Should use a connection pool (e.g., HikariCP).
    - Table schema must support job definitions and executions.
    - Must handle concurrent claims using database locks (e.g., `SELECT ... FOR UPDATE SKIP LOCKED` or a status-based update).

### REQ-2.2: Transactional Enqueue
- **Description:** Support enqueuing jobs within an existing database transaction.
- **Details:**
    - Integration with common transaction managers or a simple `Connection` pass-through.

### REQ-2.3: Retry Policy
- **Description:** Jobs should support configurable retry strategies.
- **Details:**
    - Max retries.
    - Exponential backoff with jitter.
    - Update `JobExecution` to track retry count.

### REQ-2.4: Dead-Letter Support
- **Description:** Jobs that exceed max retries should move to a `DEAD_LETTER` status.

## Success Criteria
- [ ] `JdbcJobStore` passes the same test suite as `MemoryJobStore`.
- [ ] Failed jobs are automatically retried according to their policy.
- [ ] Jobs can be scheduled durably and survive application restarts.
- [ ] PostgreSQL integration test demonstrates successful multi-worker claiming without double-execution.
