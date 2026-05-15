# Design: v0.4 - Redis Store

Status: **EXPERIMENTAL for production workloads.**

## Problem

Some deployments prefer Redis over a relational database for scheduler coordination because Redis is already available, low-latency, and operationally simple for distributed locks and queues. Khrona's `JobStore` SPI already captures the scheduler contract, but a Redis implementation needs careful key design and atomic transitions to preserve at-least-once semantics across instances.

Redis should be treated as a full scheduler store in this feature, not only as a lock layer. Mixing Redis locks with JDBC execution state would create two consistency boundaries and make failure behavior harder to reason about.

## Module Boundary

Add a new module:

```text
khrona-store-redis
```

The module should depend on:

- `khrona-core`
- **Lettuce** as the coroutine-friendly Redis client (async APIs, command timeouts, TLS/AUTH support, production maturity)
- Kotlin serialization already used by Khrona

`khrona-core`, `khrona-store-memory`, `khrona-store-jdbc`, and `khrona-ktor` must not depend on Redis.

## Public API Shape

Expose a small store type with explicit Redis connection and namespace configuration:

```kotlin
class RedisJobStore(
    private val client: RedisClient,
    private val namespace: String = "khrona",
    ...
) : JobStore
```

The exact Redis client type should be chosen during implementation after validating coroutine support and testability. The public API must let callers:

- provide connection configuration or a client they own
- set a namespace/prefix
- close resources if the store owns the client
- use the store from Ktor and standalone scheduler setups

## Redis Data Model

Use namespaced keys. Example logical keys:

- `{namespace}:jobs` for job definition JSON by job id
- `{namespace}:executions` for execution JSON by execution id
- `{namespace}:pending` sorted set by `scheduledAt`
- `{namespace}:claimed` sorted set by `expiresAt`
- `{namespace}:running` sorted set by `expiresAt`
- `{namespace}:status:{status}` sets for optional inspection by status
- `{namespace}:locks:{lockKey}` for lock ownership metadata

Normal polling must use sorted sets by score rather than scanning keys. `listEligibleExecutions(now, limit)` should read execution ids from the pending sorted set with score <= `now` and then load only those execution records.

## Atomic Transitions

Critical transitions should use Lua scripts or Redis transactions so each operation is atomic:

- claim pending execution
- heartbeat claimed/running execution
- reset expired claimed/running executions
- update terminal status
- supersede executions by lock key
- save or replace deterministic first executions

Claiming must remove the execution from `pending`, update ownership/lease fields, and add it to the claimed lease index atomically. If the execution is no longer pending, claim returns false.

### Lua Script Contracts

Critical atomic operations require Lua scripts with explicit input/output contracts:

**claimExecution(executionId, workerId, expiresAt)**
- Input: execution ID, worker ID, expiration timestamp
- Checks: execution exists in pending set, not already claimed
- On success: removes from pending, sets CLAIMED with workerId/expiresAt, adds to claimed lease index, returns true
- On failure: returns false (already claimed or not found)

**resetExpiredExecutions(now)**
- Input: current timestamp
- Scans: claimed and running lease indexes for scores < now
- For each expired: sets status to PENDING, clears workerId/expiresAt, removes from claimed/running indexes, adds to pending index
- Returns: count of reset executions

**supersedeExecutionsByLockKey(lockKey, newExecutionId)**
- Input: lock key, new execution ID to preserve
- Scans: active executions for this lock key
- For each: sets status to SUPERSEDED, removes from claimed/running indexes, updates supersedingBy field
- Preserves: new execution (must already be claimed before this script runs)
- Returns: count of superseded executions

**saveExecution(execution) - deterministic upsert**
- Input: execution JSON with deterministic ID
- Checks: if execution exists, compare createdAt to reject non-deterministic updates
- On insert: add to pending index
- On reject: return conflict error
- Returns: inserted or conflict

## Lease, Heartbeat, and Recovery

Khrona should keep explicit execution state in Redis rather than relying only on Redis key TTLs. TTLs may be useful for cleanup, but correctness should come from stored status and sorted-set lease indexes.

Heartbeat should:

- verify the execution is still `CLAIMED` or `RUNNING`
- note: current `JobStore.heartbeat(id, leaseDuration)` SPI has no workerId parameter, so ownership is inferred by execution status and ID only (matching Memory/JDBC behavior)
- update `expiresAt`
- move the execution score in the claimed/running lease index

`resetExpiredExecutions(now)` should find expired claimed/running ids by sorted-set score and reset them to `PENDING`, clearing worker and lease fields.

## Index Cleanup Rules

Every status transition must explicitly manage index membership:

| Transition | Pending | Claimed | Running | Lock Indexes | Status Indexes |
|------------|---------|---------|---------|--------------|----------------|
| PENDING → CLAIMED | Remove | Add | - | Update if lockKey | Add to CLAIMED set |
| CLAIMED → RUNNING | - | - | Add | - | Add to RUNNING set |
| RUNNING → PENDING (reset) | Add | - | Remove | Update if lockKey | Move sets |
| CLAIMED/RUNNING → SUPERSEDED | - | Remove | Remove | Clear lock ownership | Update terminal status |
| Any → COMPLETE/FAILED/DEAD_LETTER | - | Remove | Remove | Clear lock ownership | Add to terminal set |
| Retry created | Add | - | - | Update if lockKey | Add to PENDING set |

The Lua scripts for claim, reset, supersede, and terminal status must handle index cleanup atomically with the status change.

- `FORBID` should prevent overlapping active executions for the same lock key.
- `REPLACE` should supersede active executions sharing the lock key after the new execution is claimed.

Lock checks and supersede operations should be backed by Redis indexes keyed by lock key so they do not require scanning all executions.

## Payloads and Job Definitions

Use the same JSON-compatible payload conversion behavior as JDBC:

- supported values: strings, finite numbers, booleans, lists, maps with string keys, nested combinations, and null
- unsupported values fail fast

Job definitions should serialize triggers, policies, lock keys, timeout, retry policy, and metadata. Handlers remain transient and are supplied by local scheduler registration as they are today.

## Production Hardening

### Resilience and Connection Management
The Redis store uses Lettuce with a thread-safe connection and bounded request queue. `RedisJobStoreConfig` exposes:
- **Command Timeout:** Time to wait for a specific Redis command to return.
- **Request Queue Size:** Client-side backpressure boundary for queued Redis commands.
- **Reconnect Behavior:** Whether Lettuce should automatically reconnect after disconnects.
- **Shutdown Timing:** Quiet period and timeout used when the store owns the Redis client.

Separate connection pooling is intentionally not part of v0.4. Lettuce connections are thread-safe, and Khrona already bounds work with `pollBatchSize` plus Redis request queue sizing. Add pooling only after profiling shows connection contention.

### Observability
- **Logging:** Use SLF4J to log connection issues, command failures, and Lua script errors.
- **Metrics:** Optional future work: provide a listener/interceptor interface so users can plug in Micrometer or OpenTelemetry to track command latency.

### Security
- **AUTH:** Support password and username-based authentication (Redis 6+ ACLs) through Redis URI configuration.
- **TLS:** Support encrypted connections through `rediss://` Redis URI configuration.

### Error Handling
- **OOM:** Redis `OOM command not allowed` errors must be caught and propagated as a specific `KhronaRedisOomException` so applications can react.
- **Fail-Fast:** If Redis is down, the store should not block indefinitely but fail the current operation after the command timeout.

### Cleanup and Backpressure
- **Cleanup:** Implement an optional periodic cleanup task or provide clear scripts for deleting old terminal executions.
- **Backpressure:** The store relies on the scheduler's `pollBatchSize` to bound work, and the Redis client's request queue size acts as the physical backpressure boundary for concurrent store operations.

## Durability and Retention

Redis persistence depends on deployment configuration. The documentation must explain that:

- AOF/RDB settings determine whether scheduled state survives Redis restart.
- Memory eviction policies can delete scheduler keys if Redis is not configured with enough memory or an appropriate policy.
- Completed execution retention needs a clear cleanup strategy to avoid unbounded memory growth.

**v0.4 Retention Decision:** Ship with manual cleanup documentation and scripts only. Do not include automatic cleanup API. This avoids accidental data loss and keeps v0.4 scope focused. Document the recommended cleanup scripts for removing old COMPLETE/FAILED terminal records after retention period.

The first implementation should include conservative defaults and document any retention behavior. If automatic cleanup is added in a future version, it must not delete pending, claimed, running, retry, or dead-letter records unexpectedly.

## Testing Strategy

Add Redis integration tests with Testcontainers Redis. Reuse shared store behavior expectations where practical.

Coverage should include:

- save/get/list jobs
- save/get/list eligible executions with bounded limits
- concurrent claim contention across coroutines
- multi-instance scheduler execution without duplicate claims
- heartbeat and stale recovery
- lock checks and supersede behavior
- retry and dead-letter persistence
- structured payload round-trip and unsupported payload failure
- deterministic first execution upsert behavior
- namespace isolation between two stores sharing one Redis container

## Expected Tradeoffs

- Redis can reduce latency and operational friction for teams already running Redis.
- Redis persistence is configuration-dependent, so durability expectations must be explicit.
- Atomic Lua scripts add implementation complexity but keep scheduler transitions correct.
- Redis Cluster can be supported later if key hash tags and script constraints need dedicated design.
- **v0.4 is single Redis / non-cluster only.** Key naming does not use hash tags. Scripts operate on single keys or multiple keys within one namespace.
