# SPEC.md — Khrona

## Title

**Khrona** — Spec-Driven Design for a Coroutine-Native Job Runtime for Ktor

---

## 1. Vision

Khrona is a coroutine-native job scheduling and background execution runtime designed for Kotlin and deeply integrated with Ktor.

It is not merely a cron helper. It is a production-oriented runtime for:

- recurring jobs
- one-time deferred jobs
- startup lifecycle jobs
- persistent execution
- distributed coordination
- retry handling
- dead-letter handling
- operational visibility
- configuration-driven schedules

---

## 2. Product Goal

Provide a reliable, idiomatic, production-capable background execution platform for Ktor applications with:

- Kotlin-first DSL
- coroutine-native execution
- persistence and recovery
- distributed-safe coordination
- schedule configuration externalization
- observability and admin APIs

---

## 3. Non-Goals (MVP)

Khrona v1 must **not** attempt to become:

- a DAG/workflow engine
- a visual orchestration platform
- a generic event bus
- a multi-store abstraction framework
- an exactly-once execution system

Khrona must explicitly optimize for **at-least-once execution with operational correctness**.

---

## 4. Design Principles

### 4.1 Kotlin-first
The public API must feel native to Kotlin, not like Java infrastructure wrapped in extension functions.

### 4.2 Coroutine-native
All execution is based on `suspend` functions, structured concurrency, cooperative cancellation, and timeout control.

### 4.3 Code-defined logic, externally configurable schedules
Business logic and job handlers live in code. Scheduling behavior may be overridden from config or environment.

### 4.4 Production over novelty
Persistence, retries, dead-letter support, distributed coordination, and graceful shutdown are first-class requirements.

### 4.5 Honest guarantees
Khrona provides **at-least-once delivery/execution**, not magical exactly-once semantics.

---

## 5. Core Domain Model

### 5.1 JobDefinition

A `JobDefinition` describes a logical job and its handler.

#### Required fields

- `id: String`
- `description: String?`
- `handler`
- `triggerSpec`
- `policies`
- `lockKey: String?`

#### Example

```kotlin
job<EmailPayload>("send-email") {
    description = "Sends reminder emails"
    execute { payload ->
        emailService.send(payload.userId)
    }
}
```

---

### 5.2 JobExecution

Represents one concrete execution instance.

#### Fields

- `executionId: UUID`
- `jobId: String`
- `scheduledAt: Instant`
- `startedAt: Instant?`
- `completedAt: Instant?`
- `status: ExecutionStatus`
- `attempt: Int`
- `workerId: String?`
- `lockKey: String?`
- `error: String?`
- `payload: SerializedPayload?`

---

### 5.3 ExecutionStatus

```kotlin
enum class ExecutionStatus {
    PENDING,
    CLAIMED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED,
    DEAD_LETTERED
}
```

---

### 5.4 TriggerKind

```kotlin
enum class TriggerKind {
    INTERVAL,
    CRON,
    STARTUP,
    ONE_TIME
}
```

---

### 5.5 TriggerDurability

```kotlin
enum class TriggerDurability {
    EPHEMERAL,
    PERSISTENT
}
```

---

### 5.6 TriggerRepeatMode

```kotlin
enum class TriggerRepeatMode {
    RECURRING,
    ONCE_PER_STARTUP,
    ONCE_ONLY
}
```

---

## 6. Trigger Semantics

Khrona must support distinct execution semantics. These semantics must be explicit in the type system and DSL.

### 6.1 Interval Trigger

Runs repeatedly on a fixed duration.

```kotlin
job("cleanup-temp") {
    every(10.minutes)
    execute {
        cleanupService.run()
    }
}
```

#### Characteristics

- recurring
- may be persistent
- supports concurrency policies
- supports misfire handling

---

### 6.2 Cron Trigger

Runs repeatedly according to a cron expression.

```kotlin
job("nightly-report") {
    cron("0 0 2 * * *", zone = "Europe/London")
    execute {
        reports.generateNightly()
    }
}
```

#### Characteristics

- recurring
- persistent by default
- supports timezone
- supports config overrides
- supports misfire handling

---

### 6.3 Startup Trigger

Runs once for each application start, optionally after a delay.

```kotlin
job("warm-cache") {
    onStartup(delay = 30.seconds)
    execute {
        cacheService.warmUp()
    }
}
```

#### Characteristics

- executes once per app startup
- completion is **not** persisted as a permanent completed state
- a new startup creates a new eligible execution
- ideal for warmup, cache priming, reconciliation, delayed initialization

#### Failure behavior

- retries allowed during the current app lifecycle according to retry policy
- after restart, the job becomes eligible again as a fresh startup execution

---

### 6.4 Persistent One-Time Trigger

Runs exactly once in logical terms, surviving restarts.

```kotlin
job("bootstrap-search-index") {
    oncePersistent(delay = 2.minutes)
    execute {
        indexingService.bootstrap()
    }
}
```

#### Characteristics

- persisted in storage
- executes at most once after successful completion
- must not run again after success unless explicitly reset
- suitable for bootstraps, migrations, one-off business actions

#### Failure behavior

- failed executions remain retryable or failed according to policy
- restart must not create a second logical copy

---

### 6.5 Ephemeral One-Time Trigger

Runs once without durability.

```kotlin
job("probe") {
    onceEphemeral(delay = 10.seconds)
    execute {
        probeService.run()
    }
}
```

#### Characteristics

- one-time only
- not durable across restart
- if process stops before execution, it is lost

---

### 6.6 Scheduled One-Time Trigger

Runs once at a specific instant.

```kotlin
job("launch-email") {
    at(launchInstant)
    execute {
        campaignService.sendLaunchEmail()
    }
}
```

#### Characteristics

- may be persistent or ephemeral
- executes once only

---

## 7. Delay Support

Delay must be supported in the following contexts:

- `onStartup(delay = …)`
- `oncePersistent(delay = …)`
- `onceEphemeral(delay = …)`
- `enqueue(delay = …)`

This is necessary for post-startup warmups, deferred jobs, and dynamic runtime scheduling.

---

## 8. DSL Requirements

The DSL must be explicit and readable.

### 8.1 Scheduler Installation

```kotlin
install(Scheduler) {
    // For single-instance or testing
    store = MemoryJobStore() 

    // OR for persistent production
    // store = PostgresJobStore(dataSource)

    workers {
        poolSize = 8
    }
...
    shutdown {
        gracefulTimeout = 30.seconds
        cancelOnTimeout = true
    }

    configSources = listOf(
        EnvConfigSource(),
        SystemPropertyConfigSource(),
        KtorApplicationConfigSource(environment.config)
    )
}
```

---

### 8.2 Job Definitions

```kotlin
scheduler {

    job("cleanup") {
        every(10.minutes)
        execute {
            cleanupService.run()
        }
    }

    job<EmailPayload>("send-email") {
        cron("0 0 * * *")
        retry {
            maxAttempts = 3
        }
        execute { payload ->
            emailService.send(payload.userId)
        }
    }

    job("warm-cache") {
        onStartup(delay = 20.seconds)
        execute {
            cacheService.warmUp()
        }
    }

    job("bootstrap") {
        oncePersistent(delay = 2.minutes)
        execute {
            bootstrapService.run()
        }
    }
}
```

---

### 8.3 Recommended Trigger Entry Points

Khrona should expose these primary trigger functions:

- `every(...)`
- `cron(...)`
- `at(...)`
- `onStartup(...)`
- `oncePersistent(...)`
- `onceEphemeral(...)`

This avoids ambiguity and prevents runtime surprises.

---

## 9. Externalized Schedule Configuration

### 9.1 Motivation

Schedules must be modifiable without source changes or rebuilds.

This allows:
- cron changes per environment
- enabling/disabling jobs operationally
- changing delay/timezone/retry policy without redeploying code

### 9.2 Design Principle

- **job identity and handler** are defined in code
- **trigger configuration** may be overridden from config

### 9.3 Supported sources

- environment variables
- system properties
- `application.conf`
- pluggable custom configuration providers

### 9.4 Resolution precedence

The effective schedule must be resolved in the following order:

1. Environment variables
2. System properties
3. `application.conf`
4. DSL defaults

### 9.5 Example `application.conf`

```hocon
scheduler {
  jobs {

    nightly-report {
      enabled = true
      cron = "0 0 3 * * *"
      timezone = "Europe/London"
    }

    warm-cache {
      startupDelay = 30s
    }

    bootstrap-search-index {
      oncePersistentDelay = 2m
    }
  }
}
```

### 9.6 Example environment variables

```text
SCHEDULER_JOBS_NIGHTLY_REPORT_ENABLED=true
SCHEDULER_JOBS_NIGHTLY_REPORT_CRON=0 0 3 * * *
SCHEDULER_JOBS_NIGHTLY_REPORT_TIMEZONE=Europe/London

SCHEDULER_JOBS_WARM_CACHE_STARTUP_DELAY=30s
SCHEDULER_JOBS_BOOTSTRAP_SEARCH_INDEX_ONCE_PERSISTENT_DELAY=2m
```

### 9.7 Normalization rules

Job identifiers must be normalized for environment variable lookup:

- uppercase
- replace `-` with `_`
- replace `.` with `_`

Example:

- job id: `nightly-report`
- env key fragment: `NIGHTLY_REPORT`

### 9.8 Config-bound DSL

```kotlin
job("nightly-report") {
    cron(
        default = "0 0 2 * * *",
        configKey = "scheduler.jobs.nightly-report.cron"
    )
    execute {
        reports.generateNightly()
    }
}
```

### 9.9 Effective trigger introspection

The admin API and logs must expose the effective resolved schedule and the source of the resolved value.

Example:

```json
{
  "jobId": "nightly-report",
  "triggerType": "CRON",
  "configuredCron": "0 0 3 * * *",
  "source": "ENV",
  "effectiveTimezone": "Europe/London",
  "enabled": true
}
```

---

## 10. Execution Model

### 10.1 Coroutine-native execution

All jobs must be `suspend` functions.

### 10.2 Structured concurrency

Running jobs must live inside a managed coroutine scope owned by the scheduler runtime.

### 10.3 Cancellation

Cancellation must be cooperative and propagated correctly.

### 10.4 Timeouts

Each execution may define a timeout.

```kotlin
job("slow-job") {
    every(1.hours)
    timeout = 30.seconds
    execute {
        withTimeout(30.seconds) {
            service.run()
        }
    }
}
```

---

## 11. Policies

### 11.1 ConcurrencyPolicy

```kotlin
enum class ConcurrencyPolicy {
    ALLOW,
    FORBID,
    REPLACE,
    QUEUE
}
```

#### Semantics

- `ALLOW`: no concurrency restrictions
- `FORBID`: only one execution may run at a time for the lock scope
- `REPLACE`: current run is cancelled/replaced if supported
- `QUEUE`: next execution is queued for later execution

---

### 11.2 RetryPolicy

```kotlin
retry {
    maxAttempts = 5
    backoff = exponential(base = 2.seconds, max = 5.minutes, jitter = true)
}
```

#### Retry policy requirements

- max attempts
- fixed or exponential backoff
- optional jitter
- retry filtering by exception type
- dead-letter routing when exhausted

---

### 11.3 MisfirePolicy

```kotlin
enum class MisfirePolicy {
    IGNORE,
    FIRE_ONCE_NOW,
    CATCH_UP,
    CATCH_UP_LIMITED
}
```

#### Semantics

Used when recurring executions were missed due to downtime or scheduling lag.

---

## 12. Distributed Coordination

Khrona must support multi-node deployment using database-based claiming and leasing.

### 12.1 Claiming strategy

Use a persistent store with atomic claim semantics, e.g. Postgres using:

- row leasing
- `SELECT ... FOR UPDATE SKIP LOCKED`

### 12.2 Worker lease model

Each worker must heartbeat while owning running work.

If heartbeat expires, work becomes recoverable.

### 12.3 Stale execution recovery

Default behavior for stale executions:

- mark as retryable
- increment attempt
- requeue according to retry policy

If retry budget is exhausted, move to dead-letter.

---

## 13. Explicit Distributed Locking for `ConcurrencyPolicy.FORBID`

This is mandatory for correctness in clustered deployments.

### 13.1 Motivation

`FORBID` must prevent concurrent execution across nodes, not just within a single JVM.

### 13.2 Lock Key

A job may declare a `lockKey`.

```kotlin
job("sync-products") {
    every(1.minute)
    concurrency = ConcurrencyPolicy.FORBID
    lockKey = "sync-products"
    execute {
        syncService.run()
    }
}
```

### 13.3 Lock behavior

Before execution, the worker attempts to acquire a distributed lock for the relevant `lockKey`.

If a valid lock already exists:

- execution is skipped, delayed, or queued according to policy

### 13.4 Lock table

A dedicated lock table is required.

```sql
job_locks
- lock_key (PK)
- locked_by
- lock_until
```

### 13.5 Lock lifecycle

- acquire using atomic insert/update with expiration check
- renew with heartbeat/lease extension
- release on completion
- recover if holder dies and lease expires

### 13.6 Policy mapping

| Policy | Behavior |
|------|----------|
| ALLOW | No lock required |
| FORBID | Acquire global lock by `lockKey` |
| REPLACE | Cancel existing execution, then reacquire |
| QUEUE | Enqueue later execution |

---

## 14. Persistence Model

### 14.1 Required tables

- `job_definitions`
- `job_triggers`
- `job_executions`
- `job_queue`
- `job_leases`
- `job_locks`
- `dead_letter_jobs`

### 14.2 Example responsibilities

#### `job_definitions`
Stores logical job metadata.

#### `job_triggers`
Stores trigger configuration and schedule state.

#### `job_executions`
Stores execution history and runtime state.

#### `job_queue`
Stores enqueued one-time deferred work.

#### `job_leases`
Stores worker claims / ownership.

#### `job_locks`
Stores global distributed concurrency locks.

#### `dead_letter_jobs`
Stores terminal failures requiring operator action.

---

## 15. Transactional Enqueue

This is a required production feature and must not be deferred to a late roadmap phase.

### 15.1 Definition

Jobs must be enqueued atomically within the same database transaction as business operations.

### 15.2 Example

```kotlin
transaction {
    orderRepository.create(order)

    scheduler.enqueueTx(
        job = "send-order-confirmation",
        payload = OrderPayload(order.id)
    )
}
```

### 15.3 Required semantics

- same transaction / connection context
- job becomes visible only after commit
- rollback must prevent enqueue

### 15.4 Failure matrix

| Scenario | Required behavior |
|----------|-------------------|
| transaction rollback | job must not exist |
| process crash before commit | job must not exist |
| process crash after commit | job must exist and eventually run |

---

## 16. Dead-Letter Support

This is also required for early production readiness.

### 16.1 Definition

Jobs that exhaust retry limits must be moved to dead-letter storage.

### 16.2 Requirements

When retry budget is exhausted:

- execution becomes terminal
- job is copied or moved to `dead_letter_jobs`
- metadata includes payload, error, attempts, timestamps

### 16.3 Example table

```sql
dead_letter_jobs
- id
- job_id
- payload
- error
- attempts
- failed_at
```

### 16.4 DSL support

```kotlin
retry {
    maxAttempts = 5
    onFailure = DeadLetterPolicy.MOVE
}
```

### 16.5 Admin operations

Must support:

- list dead-letter jobs
- inspect dead-letter payload/error
- requeue dead-letter job
- delete dead-letter job

---

## 17. Ktor Integration

### 17.1 Plugin integration

Khrona must be installable as a Ktor plugin.

### 17.2 Lifecycle hooks

Scheduler startup must align with Ktor lifecycle.

- start on `ApplicationStarted`
- begin worker loops and schedule processing
- stop gracefully on `ApplicationStopping`

### 17.3 Admin routes

Khrona must expose optional admin routes that integrate with Ktor auth.

```kotlin
routing {
    authenticate("admin") {
        schedulerAdminRoutes()
    }
}
```

---

## 18. Graceful Shutdown

This is critical for data integrity and must be part of the base runtime semantics.

### 18.1 Requirements on `ApplicationStopping`

On shutdown the runtime must:

1. stop accepting new jobs
2. stop polling for new eligible work
3. allow in-flight jobs to complete
4. wait up to configurable graceful timeout
5. cancel remaining jobs if timeout expires
6. release or expire locks safely

### 18.2 Config example

```kotlin
install(Scheduler) {
    shutdown {
        gracefulTimeout = 30.seconds
        cancelOnTimeout = true
    }
}
```

### 18.3 Structured concurrency requirement

Workers must run within a bounded parent scope so shutdown can be coordinated cleanly.

### 18.4 Outcome matrix

| Scenario | Expected behavior |
|----------|-------------------|
| job finishes before timeout | mark success |
| job exceeds graceful timeout | cancel execution |
| node crashes abruptly | recover by lease expiration |

### 18.5 Interruption semantics

If a job is interrupted by shutdown:

- it must not be marked `SUCCESS`
- it must become `FAILED`, `CANCELLED`, or `RETRYABLE` according to policy

---

## 19. Recovery Semantics

### 19.1 Stale worker recovery

If a worker dies while holding work:

- lease eventually expires
- stale execution becomes recoverable
- lock may be reclaimed after lock expiration

### 19.2 Default recommendation

Default stale execution handling:

- retry with incremented attempt count
- honor retry policy and backoff
- move to dead-letter if retry budget exhausted

---

## 20. Observability

Observability is mandatory, not optional.

### 20.1 Metrics

Khrona must expose at least:

- `jobs_scheduled_total`
- `jobs_started_total`
- `jobs_completed_total`
- `jobs_failed_total`
- `execution_duration`
- `queue_lag`
- `retries_total`
- `dead_letter_total`
- `lock_contention_total`
- `execution_aborted_total`
- `graceful_shutdown_time`

### 20.2 Tracing

Each execution should create a trace/span with fields such as:

- `job.name`
- `execution.id`
- `scheduled.time`
- `attempt`
- `result`

### 20.3 Logging

Structured logs should include:

- `executionId`
- `jobId`
- `workerId`
- `status`
- `attempt`
- `lockKey`
- `error`

---

## 21. Administrative API

### 21.1 Read endpoints

- `GET /_scheduler/jobs`
- `GET /_scheduler/executions`
- `GET /_scheduler/executions/{id}`
- `GET /_scheduler/dead-letter`
- `GET /_scheduler/locks`

### 21.2 Action endpoints

- `POST /_scheduler/jobs/{id}/trigger`
- `POST /_scheduler/executions/{id}/cancel`
- `POST /_scheduler/executions/{id}/retry`
- `POST /_scheduler/dead-letter/{id}/requeue`
- `DELETE /_scheduler/dead-letter/{id}`

### 21.3 Admin API requirements

The admin API must expose the resolved trigger and config source, current execution state, lock state, and dead-letter visibility.

---

## 22. Validation and Invalid Configuration Policy

### 22.1 InvalidConfigPolicy

```kotlin
enum class InvalidConfigPolicy {
    FAIL_FAST,
    DISABLE_JOB,
    USE_DEFAULT_AND_WARN
}
```

### 22.2 Validation rules

The runtime must validate:

- invalid cron expressions
- negative duration values
- incompatible trigger fields
- both interval and cron simultaneously configured
- persistent one-time trigger with recurring configuration
- invalid timezone identifiers

### 22.3 Recommended default

For production-critical jobs: `FAIL_FAST`

---

## 23. Testing Requirements

Khrona must include a deterministic test toolkit.

### 23.1 Virtual time support

```kotlin
val clock = TestClock()
val scheduler = testScheduler(clock)

clock.advanceBy(10.minutes)
```

### 23.2 Required capabilities

- virtual time
- deterministic trigger advancement
- execution assertions
- failure/retry assertions
- stale lease simulation
- graceful shutdown simulation

---

## 24. Module Structure

Recommended module layout:

```text
khrona-core
khrona-ktor
khrona-store-memory
khrona-store-jdbc
khrona-cluster
khrona-observability
khrona-testkit
```

---

## 25. Operational Modes

### 25.1 In-Memory Mode
For development, tests, or single-instance applications where persistence across restarts is not required. Logic is stored in memory and lost on process termination.

### 25.2 Persistent Single-Node Mode
Durable scheduling with one active executor.

### 25.3 Distributed Mode
Shared persistence, multi-worker, multi-node coordination.

---

## 26. Guarantees

Khrona guarantees:

- at-least-once execution
- persistent recovery where configured
- retry and dead-letter handling
- lock-based distributed `FORBID` semantics
- concurrent mechanism to prevent more than one of the same job
- graceful shutdown attempts for in-flight work

Khrona does **not** guarantee:

- zero duplicates under all failure conditions
- workflow semantics beyond jobs/triggers

---

## 27. Roadmap

### v0.1
- in-memory scheduler
- Kotlin DSL
- Ktor plugin lifecycle integration
- interval and cron triggers
- startup trigger support

### v0.2
- Postgres persistence
- one-time jobs
- transactional enqueue
- dead-letter support
- retry policies
- config-driven schedule overrides

### v0.3
- distributed execution
- lease coordination
- distributed lock support for `FORBID`
- stale worker recovery

### v0.4
- Redis-backed `JobStore`
- atomic Redis claiming, heartbeat, recovery, and lock semantics
- Redis Testcontainers integration coverage
- Redis persistence, eviction, namespace, and cleanup documentation

### v0.5
- admin API
- metrics and tracing
- misfire policies
- lock inspection

### v0.6
- transactional improvements and integrations
- dead-letter tooling enhancements
- testkit hardening
- shutdown and cancellation contract tests
- claimed-before-active shutdown edge hardening
- timezone-aware cron scheduling
- job pause/disable support
- persisted job reconciliation for jobs removed from code
- operator ergonomics

### v0.7
- Android SQLite store
- Android-compatible SQLite schema and migrations
- Android SQLite store contract tests
- Android lifecycle and WorkManager integration boundary documentation

---

## 29. Future Considerations & Open Questions

The following items are identified for future discussion and refinement:

### 29.1 Payload Serialization & Evolution
- **Pluggability:** Support for JSON (Kotlinx Serialization, Jackson) or Protobuf.
- **Versioning:** Strategy for handling schema changes in payloads for long-lived or queued jobs.

### 29.2 Dependency Injection
- Formalize integration with Ktor's DI ecosystem (Koin, Kodein, etc.) to allow job handlers to access application services safely.

### 29.3 Advanced Locking & Scoping
- **Dynamic Lock Keys:** Ability to derive `lockKey` from job payloads (e.g., lock per `userId`).
- **Lock Scoping:** Clarification on whether locks are global or scoped to specific `jobId`s.

### 29.4 Error Classification
- Distinguishing between transient failures (retryable) and fatal logic errors (immediate dead-letter) to optimize resource usage.

### 29.5 Shutdown & Cancellation Contract
- **Lifecycle tests:** Add focused tests that enforce timeout cancellation, `scheduler.stop()` cancellation, handler `CancellationException` propagation, and cooperative cancellation behavior.
- **Claimed-before-active edge:** Harden or explicitly define the narrow window where an execution has been claimed but the active job coroutine has not yet been registered for shutdown coordination.
- **Documentation drift:** Keep README lifecycle wording tied to these tests so production shutdown behavior stays auditable.

### 29.6 Timezone-Aware Scheduling
- Support timezone-aware cron scheduling while preserving UTC as the default and documenting invalid timezone handling.
- Define how timezone changes interact with persisted jobs and future execution calculation.

### 29.7 Dead-Letter Queue Tooling
- Expand current `DEAD_LETTERED` execution status into operator-friendly workflows for listing, inspecting payload/error context, replaying, and cleaning up terminal executions.
- Keep the baseline portable across Memory, H2, PostgreSQL, MySQL, and Oracle stores before adding backend-specific integrations.

### 29.8 Job Pause/Disable
- Allow a job definition to be temporarily suppressed without removing it from code or persistent storage.
- Define whether paused jobs skip scheduling, skip claiming, or retain pending executions for later resume.

### 29.9 Persisted Job Reconciliation
- Detect persisted jobs and executions whose handler is no longer registered in the current code deployment.
- Define explicit operator workflows for disabling, tombstoning, deleting, or migrating removed-code jobs so stale eligible executions do not remain in storage indefinitely.
- Keep reconciliation behavior conservative by default so removing a handler from one deployment unit does not accidentally delete jobs still handled by another service or scheduler instance.

### 29.10 Backpressure & Rate Limiting
- Strategies for global or per-job rate limiting to prevent database and worker exhaustion during spikes.

### 29.11 Multi-tenancy
- Context propagation for tenant-aware applications (e.g., `TenantId` in `CoroutineContext` or MDC).

### 29.12 Admin API Security
- Role-Based Access Control (RBAC) for the Admin API to distinguish between read-only monitoring and destructive operations (e.g., deleting dead-letter jobs).

### 29.13 Memory Efficiency & History Pruning
- **Execution History Growth:** Implement pruning or TTL (Time-To-Live) policies for `JobExecution` records in memory and persistent stores to prevent unbounded memory growth over time.
- **Eviction Strategy:** Define strategies for archiving or deleting `SUCCESS` and `FAILED` executions to maintain optimal performance for long-running application instances.

### 29.14 Job Isolation & Resilience
- **Failure Impact:** Ensure strict isolation of job failures using structured concurrency and supervisor jobs so that individual job crashes never compromise the stability of the host Ktor application.
- **Resource Limits:** Explore per-job memory or execution time limits to prevent rogue jobs from exhausting system resources.
