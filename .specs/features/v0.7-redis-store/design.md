# Design: v0.7 - Redis Store

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
- a coroutine-friendly Redis client selected during implementation
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

## Lease, Heartbeat, and Recovery

Khrona should keep explicit execution state in Redis rather than relying only on Redis key TTLs. TTLs may be useful for cleanup, but correctness should come from stored status and sorted-set lease indexes.

Heartbeat should:

- verify the execution is still `CLAIMED` or `RUNNING`
- verify the worker still owns it when ownership is available
- update `expiresAt`
- move the execution score in the claimed/running lease index

`resetExpiredExecutions(now)` should find expired claimed/running ids by sorted-set score and reset them to `PENDING`, clearing worker and lease fields.

## Lock Semantics

The store must preserve existing lock-key behavior:

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
The Redis store should use a client that supports connection pooling (e.g., Lettuce or Jedis). The configuration must expose:
- **Connection Timeout:** Time to wait for a connection to be established.
- **Command Timeout:** Time to wait for a specific Redis command to return.
- **Pool Sizing:** Min/Max idle and active connections.
- **Reconnection Policy:** Exponential backoff for lost connections.

### Observability
- **Logging:** Use SLF4J to log connection issues, command failures, and Lua script errors.
- **Metrics:** Provide an optional listener/interceptor interface so users can plug in Micrometer or OpenTelemetry to track command latency and pool usage.

### Security
- **AUTH:** Support password and username-based authentication (Redis 6+ ACLs).
- **TLS:** Support encrypted connections with configurable trust stores.

### Error Handling
- **OOM:** Redis `OOM command not allowed` errors must be caught and propagated as a specific `KhronaRedisOomException` so applications can react.
- **Fail-Fast:** If Redis is down, the store should not block indefinitely but fail the current operation after the command timeout.

### Cleanup and Backpressure
- **Cleanup:** Implement an optional periodic cleanup task or provide clear scripts for deleting old terminal executions.
- **Backpressure:** The store relies on the scheduler's `pollBatchSize` to bound work, but the Redis client pool acts as the physical backpressure boundary for concurrent store operations.

## Durability and Retention

Redis persistence depends on deployment configuration. The documentation must explain that:

- AOF/RDB settings determine whether scheduled state survives Redis restart.
- Memory eviction policies can delete scheduler keys if Redis is not configured with enough memory or an appropriate policy.
- Completed execution retention needs a clear cleanup strategy to avoid unbounded memory growth.

The first implementation should include conservative defaults and document any retention behavior. If automatic cleanup is added, it must not delete pending, claimed, running, retry, or dead-letter records unexpectedly.

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
