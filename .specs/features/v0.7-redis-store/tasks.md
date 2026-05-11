# Tasks: v0.7 - Redis Store

## Phase 1: Module and Client Setup

- [ ] **Task 1.1:** Add `khrona-store-redis` to Gradle settings.
- [ ] **Task 1.2:** Choose and add a coroutine-friendly Redis client dependency after validating Kotlin/JVM compatibility.
- [ ] **Task 1.3:** Add Redis Testcontainers dependency and test fixture setup.
- [ ] **Task 1.4:** Define `RedisJobStore` public constructor/configuration, including namespace and resource ownership.

## Phase 2: Redis Data Model

- [ ] **Task 2.1:** Define key namespace conventions for jobs, executions, pending index, lease indexes, status indexes, and lock indexes.
- [ ] **Task 2.2:** Implement job definition save/get/list operations.
- [ ] **Task 2.3:** Implement execution serialization using the same structured JSON payload behavior as JDBC.
- [ ] **Task 2.4:** Add namespace isolation tests using two stores against one Redis instance.

## Phase 3: Execution Queue and Claiming

- [ ] **Task 3.1:** Implement `saveExecution` with deterministic-id upsert behavior and pending sorted-set indexing.
- [ ] **Task 3.2:** Implement bounded `listEligibleExecutions(now, limit)` using sorted-set score lookup.
- [ ] **Task 3.3:** Implement atomic `claimExecution` with ownership and lease updates.
- [ ] **Task 3.4:** Add concurrent claim contention tests proving only one worker claims a pending execution.
- [ ] **Task 3.5:** Add multi-scheduler tests proving interval, cron, one-time, and manual executions are not duplicated.

## Phase 4: Lease, Heartbeat, and Recovery

- [ ] **Task 4.1:** Implement heartbeat updates with owner/status validation.
- [ ] **Task 4.2:** Implement `resetExpiredExecutions(now)` using lease sorted sets.
- [ ] **Task 4.3:** Add tests for running execution recovery after heartbeat stops.
- [ ] **Task 4.4:** Add tests for configurable lease and heartbeat behavior with Redis.

## Phase 5: Locking and Replacement

- [ ] **Task 5.1:** Implement `isLockHeld(lockKey, excludeExecutionId)` using Redis lock indexes.
- [ ] **Task 5.2:** Implement `supersedeExecutionsByLockKey` atomically.
- [ ] **Task 5.3:** Add tests for `FORBID` overlap prevention across scheduler instances.
- [ ] **Task 5.4:** Add tests for `REPLACE` superseding active executions without superseding the newly claimed execution.

## Phase 6: Retries, Misfires, and Terminal State

- [ ] **Task 6.1:** Verify retry execution persistence and payload propagation.
- [ ] **Task 6.2:** Verify dead-letter terminal state persistence.
- [ ] **Task 6.3:** Verify `MisfirePolicy.FIRE_NOW` and `IGNORE` behavior using Redis.
- [ ] **Task 6.4:** Verify recurring next-run persistence after success and terminal failure.

## Phase 7: Production Hardening

- [ ] **Task 7.1:** Implement `RedisJobStoreConfig` with timeouts, pool sizing, and retry settings.
- [ ] **Task 7.2:** Implement Redis AUTH and TLS support.
- [ ] **Task 7.3:** Implement `KhronaRedisOomException` and fail-fast command handling.
- [ ] **Task 7.4:** Implement `AutoCloseable` for the store to ensure clean connection pool shutdown.
- [ ] **Task 7.5:** Add logging for Lua script errors and connection failures.
- [ ] **Task 7.6:** (Optional) Add a basic observability interface for command latency.

## Phase 8: Documentation and Operations

- [ ] **Task 8.1:** Update README with Redis dependency/setup example.
- [ ] **Task 8.2:** Update architecture docs to include `RedisJobStore`.
- [ ] **Task 8.3:** Add Redis performance and operational guidance covering persistence, eviction policies, namespaces, cleanup, and connection management.
- [ ] **Task 8.4:** Document Redis durability tradeoffs compared with JDBC.

## Verification

- [ ] Redis store passes shared `JobStore` behavior tests.
- [ ] Multi-instance scheduler tests pass against Redis.
- [ ] Namespace isolation works.
- [ ] Bounded polling does not scan all execution keys.
- [ ] Atomic claiming prevents duplicate execution.
- [ ] Heartbeat and stale recovery match existing scheduler semantics.
- [ ] Structured payload behavior matches JDBC.
- [ ] Memory and JDBC store tests remain green.
- [ ] Full suite passes with `./gradlew clean test`.
