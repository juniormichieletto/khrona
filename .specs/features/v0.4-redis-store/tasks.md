# Tasks: v0.4 - Redis Store

## Current Implementation Status

Last updated after the Redis namespace and atomic claim slice.

Implemented and verified:
- `khrona-store-redis` module is included in Gradle.
- Lettuce Redis client dependency is configured.
- Redis Testcontainers test harness is in place.
- `RedisJobStore` supports the shared `JobStore` contract for job persistence, execution persistence, bounded eligible lookup, claiming, heartbeat, stale recovery, lock inspection, status updates, and structured payload round trips.
- Namespace isolation is covered with two Redis stores sharing one Redis instance.
- `claimExecution` uses a Lua script to atomically validate claimability, move ownership, update lease metadata, and clean pending/claimed/running/lock indexes.
- Concurrent claim contention test proves only one worker wins a pending execution claim.
- Stale Redis claim-lock keys no longer block a pending execution whose persisted state is claimable.
- Unsupported payload values fail fast like JDBC.
- `./gradlew clean test` passes with the Redis module included.

Important caveats:
- `supersedeExecutionsByLockKey` is functional for the current store contract but Task 5.2 remains open because it is not yet implemented as an atomic Redis script with dedicated tests.
- Redis multi-scheduler behavior, retry/DLQ/misfire scenarios, and README/operations docs are still pending.

Next resume step:
- Start with Task 3.5 or Task 5.2. Recommended path: add Redis multi-scheduler coverage next, then make `supersedeExecutionsByLockKey` atomic with a dedicated Lua script and replacement tests.

## Phase 1: Module and Client Setup

- [x] **Task 1.1:** Add `khrona-store-redis` to Gradle settings.
- [x] **Task 1.2:** Add Lettuce Redis client dependency (async/coroutine support, TLS/AUTH, command timeouts).
- [x] **Task 1.3:** Add Redis Testcontainers dependency and test fixture setup.
- [x] **Task 1.4:** Define `RedisJobStore` public constructor/configuration, including namespace and resource ownership.
- [x] **Task 1.5:** Create failing `RedisJobStoreContract` test against empty implementation.

## Phase 2: Redis Data Model

- [x] **Task 2.1:** Define key namespace conventions for jobs, executions, pending index, lease indexes, status indexes, and lock indexes.
- [x] **Task 2.2:** Implement job definition save/get/list operations.
- [x] **Task 2.3:** Implement execution serialization using the same structured JSON payload behavior as JDBC.
- [x] **Task 2.4:** Add namespace isolation tests using two stores against one Redis instance.
- [x] **Task 2.5:** Document index cleanup rules for each status transition (pending, claimed, running, lock, status sets).

## Phase 3: Execution Queue and Claiming

- [x] **Task 3.1:** Implement `saveExecution` with deterministic-id upsert behavior and pending sorted-set indexing.
- [x] **Task 3.2:** Implement bounded `listEligibleExecutions(now, limit)` using sorted-set score lookup.
- [x] **Task 3.3:** Implement atomic `claimExecution` Lua script with ownership, lease updates, and index cleanup.
- [x] **Task 3.4:** Add concurrent claim contention tests proving only one worker claims a pending execution.
- [ ] **Task 3.5:** Add multi-scheduler tests proving interval, cron, one-time, and manual executions are not duplicated.

## Phase 4: Lease, Heartbeat, and Recovery

- [x] **Task 4.1:** Implement heartbeat updates with owner/status validation.
- [x] **Task 4.2:** Implement `resetExpiredExecutions(now)` using lease sorted sets.
- [ ] **Task 4.3:** Add tests for running execution recovery after heartbeat stops.
- [ ] **Task 4.4:** Add tests for configurable lease and heartbeat behavior with Redis.

## Phase 5: Locking and Replacement

- [x] **Task 5.1:** Implement `isLockHeld(lockKey, excludeExecutionId)` using Redis lock indexes.
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
- [ ] **Task 8.3:** Add Redis performance and operational guidance covering persistence, eviction policies, namespaces, cleanup scripts, and connection management.
- [ ] **Task 8.4:** Document Redis durability tradeoffs compared with JDBC.
- [ ] **Task 8.5:** Document v0.4 cleanup strategy (manual scripts only, no automatic cleanup API).

## Verification

- [x] Redis store passes shared `JobStore` behavior tests.
- [ ] Multi-instance scheduler tests pass against Redis.
- [x] Namespace isolation works.
- [x] Bounded polling does not scan all execution keys.
- [x] Atomic claiming prevents duplicate execution.
- [x] Heartbeat and stale recovery match existing scheduler semantics.
- [x] Structured payload behavior matches JDBC.
- [x] Memory and JDBC store tests remain green.
- [x] Full suite passes with `./gradlew clean test`.
