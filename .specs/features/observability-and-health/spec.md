# Observability and Health (`OBS-1`)

## Overview

Khrona currently propagates `correlationId` through SLF4J MDC and writes structured log lines, but applications running Khrona in production have no built-in way to emit metrics, create distributed tracing spans, inspect scheduler loop and store health, or preserve rich failure diagnostics without losing error types.

This feature closes acceptance area **OBS-1** from `docs/1.0-readiness.md` and aligns with `.specs/project/PROJECT.md` Section 20 by introducing:

1. A zero-dependency observability SPI (`KhronaObserver`) and failure diagnostic formatter in `khrona-core`.
2. Scheduler loop and store health inspection (`Scheduler.health()` and `JobStore.checkHealth()`) plus bounded execution counting (`JobStore.countExecutions()`).
3. An optional `khrona-observability` module shipping Micrometer (`MicrometerKhronaObserver`) and OpenTelemetry (`OpenTelemetryKhronaObserver`) adapters.

## Goal

Provide production-grade metrics, distributed tracing hooks, scheduler/store readiness signals, and safe failure diagnostics while keeping `khrona-core` dependency-free and keeping HTTP routes, dashboards, and access policies owned by the host application.

## Requirements

- **REQ-OBS1: Zero-Dependency Core SPI & Isolation:** `khrona-core` exposes `KhronaObserver` without adding Micrometer, OpenTelemetry, or HTTP server dependencies. Any exception thrown by an observer (other than `CancellationException` propagated from the wrapped handler itself) must be caught and logged so observability failures never fail job executions, block heartbeats, or abort the scheduler polling loop.
- **REQ-OBS2: Required Runtime Signals:** Khrona emits callbacks covering:
  - scheduling lag (`max(0, claimedOrStartedAt - scheduledAt)`)
  - execution duration and terminal/interrupted outcome (`SUCCESS`, `FAILED_RETRY`, `DEAD_LETTERED`, `MISFIRED`, `SUPERSEDED`, `CANCELLED`, `SHUTDOWN_CHECKPOINT`)
  - scheduled executions (`INITIAL_SEED`, `RECURRING_NEXT`, `RETRY`, `MANUAL_TRIGGER`)
  - retries scheduled and dead-letter transitions
  - heartbeat renewals and failures (`LOST_OWNERSHIP_OR_EXPIRED`, `STORE_ERROR`, `LEASE_BUDGET_EXCEEDED`)
  - stale execution recoveries (`resetExpiredExecutions`)
  - lock contention (`FORBID` lock held before or after claim)
  - dead-letter queue size (`ExecutionStatus.DEAD_LETTERED` count across the store)
  - retention cleanup purged count
  - graceful shutdown duration and completion mode (clean vs forced timeout cancellation)
  - optional store operation latency hook (`onStoreOperation`) for built-in or decorated stores
- **REQ-OBS3: Bounded Metric Cardinality:** Metric tags/labels emitted by built-in adapters must stay strictly bounded (`job_id`, `outcome`, `error_type`, `reason`, `policy`, `operation`, `status`). Unbounded identifiers (`executionId`, `correlationId`, `workerId`, raw error messages, and payload contents) must never be added as metric tags and belong exclusively in logs and trace span attributes.
- **REQ-OBS4: Tracing & Context Propagation Hook:** `KhronaObserver` provides a suspendable `aroundExecution(context, block)` hook so adapters (such as OpenTelemetry) can open execution spans and propagate coroutine `CoroutineContext` elements across handler execution and nested coroutines.
- **REQ-OBS5: Scheduler & Store Health/Readiness:** `Scheduler.health()` returns a snapshot (`SchedulerHealth`) combining scheduler loop liveness (`STOPPED`, `STARTING`, `UP`, `DEGRADED`, `DOWN`) and store connectivity (`JobStore.checkHealth(): StoreHealth`). Documentation and tests must cover startup, controlled store outage (`DEGRADED` / `DOWN`), and automatic recovery back to `UP` once the store recovers.
- **REQ-OBS6: Host-Owned HTTP Surface:** Khrona must not ship built-in `/health`, `/ready`, or `/metrics` Ktor routes or security filters. Host applications own HTTP exposure, status codes, and access policy using `Scheduler.health()` and their configured `MeterRegistry` / `OpenTelemetry`.
- **REQ-OBS7: Failure Diagnostics & Redaction:** Persisted execution errors and observer failure events preserve exception type information (`ClassSimpleName: message`, or `ClassSimpleName` when `message` is null/blank) through `ErrorDiagnosticFormatter`, with a configurable maximum length and redaction hook. Documentation must advise against embedding secrets or sensitive PII in exception messages or payloads.
- **REQ-OBS8: Optional Adapter Module (`khrona-observability`):** Add a standalone `khrona-observability` Gradle module containing `MicrometerKhronaObserver` and `OpenTelemetryKhronaObserver` with `compileOnly` dependencies on Micrometer and OpenTelemetry APIs so consumers bring only the telemetry stack they use.
- **REQ-OBS9: Verification & E2E Evidence:** Cover unit behavior, store contracts (`checkHealth` and `countExecutions`), bounded-label assertions, Micrometer/OpenTelemetry adapter behavior, and extended Dockerized Ktor E2E scenarios verifying successful/failing execution telemetry and health state transitions during a controlled store outage and recovery.

## Non-Goals

- Built-in Ktor HTTP routes (`/metrics`, `/health`, `/ready`) or Prometheus/OTLP exporters/collectors.
- Built-in alerting rules, Grafana dashboards, or admin UI.
- Unbounded per-execution metric dimensions or payload-derived metric labels.
- Requiring Micrometer or OpenTelemetry at runtime for consumers that do not use `khrona-observability`.
- Distributed cross-node metric aggregation inside Khrona itself (metrics are emitted per node and aggregated by the host telemetry backend).

## Success Criteria

- A host application using `khrona-core` alone can attach a custom `KhronaObserver` and query `scheduler.health()` with zero additional transitive dependencies.
- A host application adding `khrona-observability` can register `MicrometerKhronaObserver` and/or `OpenTelemetryKhronaObserver` in `Khrona { observer = ... }` and observe lag, duration, outcomes, retries, lease/heartbeat failures, stale recoveries, lock contention, retention cleanup, shutdown duration, and DLQ size.
- Metric label checks confirm `executionId`, `correlationId`, `workerId`, and raw exception messages never appear in Micrometer tags or OTel metric attributes, while trace spans include `khrona.execution.id`, `khrona.job.id`, `khrona.attempt`, `khrona.scheduled_at`, and `khrona.correlation_id`.
- `Scheduler.health()` transitions predictably through `STARTING` -> `UP` -> `DEGRADED`/`DOWN` during a simulated store outage -> `UP` after recovery, and reports `STOPPED` after `scheduler.stop()`.
- `JobStore.countExecutions` and `JobStore.checkHealth` pass shared `JobStoreContract` tests across Memory, JDBC (H2, PostgreSQL, MySQL, Oracle), and Redis.
- Dockerized Ktor E2E tests verify execution metrics/outcomes and `/health` readiness transitions during a controlled store outage inside generated Ktor applications.
