# Spec: v0.7 - Redis Store

## Overview

Khrona currently supports in-memory storage and JDBC-backed durable storage. Redis is a strong fit for deployments that want low-latency scheduling coordination, distributed claiming, and short operational setup without introducing a relational database just for scheduler state.

This feature will add a Redis-backed implementation of the `JobStore` SPI so Khrona can coordinate scheduled executions across multiple application instances using Redis data structures while preserving the existing scheduler contract.

## Objectives

- Provide a Redis-backed `JobStore` for JVM/server deployments.
- Preserve Khrona's at-least-once execution model.
- Support multi-instance claiming, lease recovery, heartbeats, retries, misfires, lock keys, and recurring execution persistence.
- Keep Redis-specific dependencies outside `khrona-core`.
- Reduce idle and claim latency compared with relational polling where Redis is the chosen coordination backend.
- Document Redis durability and operational tradeoffs clearly.

## Requirements

- **REQ-RS1: Redis Store Module:** Add a separate `khrona-store-redis` module that depends on `khrona-core` and a coroutine-friendly Redis client.
- **REQ-RS2: JobStore Compatibility:** The Redis store must implement the same `JobStore` behavior required by the scheduler, including job definition persistence, execution enqueueing, bounded eligible listing, claiming, status updates, heartbeat, stale execution recovery, lock checks, supersede behavior, retry/dead-letter state, and structured payload persistence.
- **REQ-RS3: Atomic Claims:** Claiming an execution must be atomic across scheduler instances so the same pending execution cannot be claimed by more than one worker.
- **REQ-RS4: Lease and Heartbeat:** Claimed and running executions must keep `workerId`, `startedAt`, and `expiresAt` state, and `heartbeat` must extend the lease only while the execution is still owned.
- **REQ-RS5: Stale Recovery:** Expired `CLAIMED` or `RUNNING` executions must be discoverable and reset back to `PENDING` according to existing scheduler semantics.
- **REQ-RS6: Lock Semantics:** `FORBID` and `REPLACE` lock-key behavior must match existing Memory and JDBC store behavior.
- **REQ-RS7: Recurring and Retry Persistence:** Next recurring executions, retry executions, misfired executions, and terminal statuses must survive application restart as long as Redis persistence/retention keeps the data.
- **REQ-RS8: Payload Compatibility:** Redis payload serialization must match JDBC structured JSON behavior and fail fast for unsupported payload values.
- **REQ-RS9: Key Namespace:** The store must require or default a configurable namespace/prefix so multiple apps or environments can share a Redis deployment safely.
- **REQ-RS10: Bounded Operations:** Eligible execution listing must respect `pollBatchSize` and avoid scanning the full keyspace on each scheduler poll.
- **REQ-RS11: Cleanup Guidance:** Documentation must explain how completed, misfired, failed, and dead-lettered executions are retained or cleaned up.
- **REQ-RS12: Test Coverage:** Add Redis store contract tests using Testcontainers Redis and shared behavior tests comparable to Memory and JDBC.

## Non-Goals

- Replacing JDBC as the default production store.
- Guaranteeing exactly-once execution.
- Implementing Redis Cluster-specific sharding optimizations in the first version.
- Requiring Redis Streams, Pub/Sub, or keyspace notifications for baseline correctness.
- Using Redis as a sidecar lock provider for JDBC jobs in the first version.
- Adding admin UI features for Redis-specific inspection in this feature.

## Success Criteria

- Applications can configure `khrona-store-redis` as the scheduler store.
- Multiple scheduler instances can use the same Redis namespace without duplicate claims for the same execution.
- Redis store behavior matches existing scheduler semantics for interval, cron, one-time, manual, retry, misfire, lease, heartbeat, and lock scenarios.
- Store operations are bounded and do not require full Redis key scans during normal polling.
- Redis persistence and retention tradeoffs are documented.
- Memory and JDBC stores remain unchanged and their tests stay green.
