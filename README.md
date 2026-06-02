# Khrona

**Khrona** is a coroutine-native job scheduling and background execution runtime designed for Kotlin and deeply integrated with Ktor.

It provides a reliable, idiomatic, and production-capable platform for background tasks, ranging from simple recurring jobs to distributed-safe persistent executions.

> **Etymology:** The name **Khrona** is inspired by **Chronos** (Ancient Greek: χρόνος, “time”), the personification of time and the root of modern terms like “chronology.” The stylized spelling reflects a modern, system-oriented interpretation of temporal control and scheduling.

## Table of Contents

- [Features](#features)
- [Architecture at a Glance](#architecture-at-a-glance)
- [Job Execution Flow](#job-execution-flow)
- [Installation](#installation)
- [Quick Start (In-Memory)](#quick-start-in-memory)
- [Persistent Storage (JDBC)](#persistent-storage-jdbc)
- [Persistent Storage (Redis)](#persistent-storage-redis)
  - [Local Redis for Development](#local-redis-for-development)
- [MySQL 8 & Multi-Node Testing](#mysql-8--multi-node-testing)
  - [Local Multi-Instance Setup](#local-multi-instance-setup)
- [Advanced Configuration](#advanced-configuration)
  - [Retry Policies](#retry-policies)
  - [Concurrency & Locking](#concurrency--locking)
  - [Long-Running Jobs, Timeouts, and Restarts](#long-running-jobs-timeouts-and-restarts)
  - [Graceful Shutdown and Cancellation](#graceful-shutdown-and-cancellation)
  - [Structured Payloads](#structured-payloads)
  - [Misfire Policies](#misfire-policies)
  - [Correlation ID & Observability](#correlation-id--observability)
- [Manual Control (Standalone)](#manual-control-standalone)
- [Trigger Formats](#trigger-formats)
  - [Cron Trigger (Unix Format)](#cron-trigger-unix-format)
  - [Interval Trigger](#interval-trigger)
- [Roadmap](#roadmap)
- [License](#license)

## Features

- **🚀 Coroutine-native:** Built on Kotlin coroutines for efficient, non-blocking execution.
- **🛠️ Fluent DSL:** Define jobs and schedules using a clean, Kotlin-first DSL.
- **⏱️ Flexible Triggers:**
  - **Interval:** Run jobs at fixed durations (e.g., every 5 minutes).
  - **Cron:** Standard Unix-format cron expressions (`* * * * *`) with optional per-job timezones.
- **🛡️ Reliability & Resilience:**
  - **Retry Policies:** Configurable exponential backoff with jitter.
  - **Concurrency Control:** Manage overlapping executions with `FORBID` or `REPLACE` policies.
  - **Misfire Policies:** Handle delayed executions (FIRE_NOW or IGNORE).
- **💾 Persistence Support:** Durable job storage using JDBC (PostgreSQL, MySQL, Oracle, H2).
- **⚡ Redis Store (Experimental):** Redis-backed coordination with atomic claiming, leases, recovery, and lock replacement.
- **🌐 Distributed Ready:** Multi-node deployment support with deterministic IDs and distributed locking.
- **🔌 Ktor Integration:** First-class Ktor plugin with seamless lifecycle management.

## Architecture at a Glance

```mermaid
graph TD
    subgraph "Client Layer"
        DSL[Kotlin DSL] --> Ktor[Ktor Plugin]
    end

    subgraph "Core Engine"
        Sch[Scheduler] --> Workers[Execution Coroutines]
        Workers --> Heartbeat[Heartbeat Manager]
    end

    subgraph "Storage Layer (SPI)"
        Store[JobStore Interface]
        Memory[MemoryJobStore] --- Store
        JDBC[JdbcJobStore] --- Store
        Redis[RedisJobStore] --- Store
        
        subgraph "JDBC Dialects"
            JDBC --- Postgres[PostgreSQL]
            JDBC --- MySql[MySQL 8]
            JDBC --- Oracle[Oracle]
            JDBC --- H2[H2]
        end

        Redis --- RedisDB[Redis]
    end

    DSL --> Sch
    Sch <--> Store
    Workers <--> Store
```

> For a detailed breakdown of the execution flow and sequence diagrams, see the [Full Architecture Document](.specs/codebase/ARCHITECTURE.md). For scheduler and database load guidance, see the [Performance Guide](.specs/codebase/PERFORMANCE.md).

## Job Execution Flow

The scheduler coordinates every execution through the configured `JobStore`. With `JdbcJobStore`, each store call shown below becomes a database query or update against `khrona_jobs` or `khrona_executions`. With `RedisJobStore`, the same contract is implemented with hashes, sorted sets, lock sets, and Lua scripts.

```mermaid
flowchart TD
    Start["Scheduler starts or job is registered"] --> SaveJob["Store job definition<br/>saveJob"]
    SaveJob --> FindFirst["Calculate first scheduled time"]
    FindFirst --> Existing{"Execution ID already exists?<br/>getExecution"}
    Existing -- "No" --> SavePending["Persist PENDING execution<br/>saveExecution"]
    Existing -- "Yes" --> PollWait
    SavePending --> PollWait["Wait for fixed polling interval"]
    Manual["Manual trigger(jobId, payload)"] --> LoadManual["Load job definition<br/>getJob"]
    LoadManual --> SaveManual["Persist immediate PENDING execution<br/>with a random ID<br/>saveExecution"]
    SaveManual --> PollWait

    PollWait --> RecoveryDue{"Stale recovery due?<br/>checked about once per minute"}
    RecoveryDue -- "Yes" --> Recover["Reset expired CLAIMED or RUNNING<br/>executions to PENDING<br/>resetExpiredExecutions"]
    RecoveryDue -- "No" --> Fetch
    Recover --> Fetch["Fetch due work up to pollBatchSize<br/>PENDING or expired CLAIMED/RUNNING<br/>listEligibleExecutions"]

    Fetch --> Eligible{"Eligible execution found?"}
    Eligible -- "No" --> PollWait
    Eligible -- "Yes" --> Handler{"Local handler registered?"}
    Handler -- "No: another node may own the handler" --> NextEligible["Check next fetched execution"]
    Handler -- "Yes" --> FreshJob["Re-fetch latest job definition<br/>getJob"]

    FreshJob --> JobFound{"Job definition exists?"}
    JobFound -- "No" --> NextEligible
    JobFound -- "Yes" --> Misfire{"Older than misfireThreshold?"}
    Misfire -- "Yes + IGNORE" --> ClaimMisfire["Atomically claim ignored misfire<br/>claimExecution"]
    ClaimMisfire -- "Won claim" --> MarkMisfire["Mark MISFIRED and persist deterministic<br/>next run calculated from current time<br/>updateExecutionStatus + saveExecution"]
    ClaimMisfire -- "Lost claim" --> NextEligible
    Misfire -- "No or FIRE_NOW" --> Policy{"Concurrency policy"}
    MarkMisfire --> NextEligible

    Policy -- "ALLOW or REPLACE" --> Claim
    Policy -- "FORBID" --> PreLock{"Active unexpired lock exists?<br/>isLockHeld"}
    PreLock -- "Yes" --> NextEligible
    PreLock -- "No" --> Claim["Atomically claim with worker ID and lease<br/>PENDING or expired active -> CLAIMED<br/>claimExecution"]

    Claim --> Claimed{"Claim succeeded?"}
    Claimed -- "No: another node won" --> NextEligible
    Claimed -- "Yes + REPLACE" --> Supersede["Mark older active executions with same lock<br/>as SUPERSEDED and cancel local coroutines<br/>supersedeExecutionsByLockKey"]
    Claimed -- "Yes + ALLOW" --> Launch
    Claimed -- "Yes + FORBID" --> PostLock{"Does another active execution<br/>hold the same lock?<br/>isLockHeld excluding current ID"}
    Supersede --> Launch
    PostLock -- "Yes: race detected" --> Release["Release current claim back to PENDING<br/>updateExecutionStatus"]
    PostLock -- "No" --> Launch["Launch execution coroutine"]
    Release --> NextEligible

    Launch --> Running["Mark RUNNING<br/>updateExecutionStatus"]
    Running --> Heartbeat["While handler runs, extend lease periodically<br/>heartbeat"]
    Running --> Execute["Run handler<br/>optionally within timeout"]
    Heartbeat -- "Lease update rejected" --> Cancel["Cancel handler: execution was reclaimed<br/>or superseded"]

    Execute -- "Success" --> Success["Mark SUCCESS<br/>updateExecutionStatus"]
    Execute -- "Failure or timeout" --> Retry{"Retry attempts remain?"}
    Execute -- "Cancellation" --> Cancel
    Retry -- "Yes" --> Failed["Mark FAILED and persist a new<br/>PENDING retry with backoff"]
    Retry -- "No" --> Dead["Mark DEAD_LETTERED"]
    Success --> Recurring{"Recurring trigger has a next run?"}
    Dead --> Recurring
    Failed --> NextEligible
    Recurring -- "Yes" --> SaveNext["Persist next deterministic<br/>PENDING execution if absent"]
    Recurring -- "No" --> NextEligible
    SaveNext --> NextEligible
    Cancel --> NextEligible
    NextEligible --> MoreFetched{"More fetched executions?"}
    MoreFetched -- "Yes" --> Eligible
    MoreFetched -- "No: batch exhausted" --> PollWait
```

The lock is represented by active execution state, not by a separate scheduler-owned mutex. An execution holds its `lockKey` while it is `CLAIMED` or `RUNNING` and its lease is still valid. Heartbeats extend that lease. If a worker crashes or stops heartbeating, the execution expires and can be reclaimed; periodic stale recovery also moves expired active executions back to `PENDING`.

Recurring scheduling intentionally uses two reference points. When `MisfirePolicy.IGNORE` skips an overdue execution, Khrona calculates the replacement from the current time so it does not replay the missed cadence. After `SUCCESS` or terminal `DEAD_LETTERED` failure, Khrona calculates the next run from the previous `scheduledAt` value to preserve the recurring cadence.

For JDBC, the main database access points are:

- `listEligibleExecutions`: reads due `PENDING` rows and expired `CLAIMED` or `RUNNING` rows, ordered by `scheduled_at` and bounded by `pollBatchSize`.
- `claimExecution`: performs an atomic conditional update to `CLAIMED`; PostgreSQL and MySQL use `FOR UPDATE SKIP LOCKED` so only one competing worker wins.
- `isLockHeld`: checks for another unexpired `CLAIMED` or `RUNNING` row with the same `lock_key`.
- `heartbeat`: updates `expires_at` only while the row remains `CLAIMED` or `RUNNING`.
- `resetExpiredExecutions`: returns expired active rows to `PENDING` so work can resume after a crash or lost worker.
- `supersedeExecutionsByLockKey`: transactionally marks older active rows as `SUPERSEDED` for `ConcurrencyPolicy.REPLACE`.

## Installation

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.github.juniormichieletto:khrona-ktor:0.4.0")
    
    // Choose your storage:
    implementation("io.github.juniormichieletto:khrona-store-memory:0.4.0") // For dev/testing
    implementation("io.github.juniormichieletto:khrona-store-jdbc:0.4.0")   // For production
    // implementation("io.github.juniormichieletto:khrona-store-redis:0.4.0") // Experimental Redis coordination
}
```

## Quick Start (In-Memory)

The fastest way to get started with Ktor using ephemeral in-memory storage.

```kotlin
import io.khrona.ktor.*
import io.khrona.store.memory.MemoryJobStore
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.minutes

fun Application.module() {
    install(Khrona) {
        store = MemoryJobStore()
    }

    launch {
        scheduler {
            job("heartbeat") {
                every(1.minutes)
                execute {
                    println("Khrona is alive!")
                }
            }
        }
    }
}
```

## Persistent Storage (JDBC)

For production use, jobs should persist across application restarts.

```kotlin
import io.khrona.store.jdbc.JdbcJobStore
import io.khrona.store.jdbc.PostgresDialect // Or MySqlDialect, H2Dialect, etc.
import kotlinx.coroutines.runBlocking

val store = JdbcJobStore(dataSource, PostgresDialect())
runBlocking { store.migrate() }

install(Khrona) {
    this.store = store
}
```

`migrate()` is suspend and fail-fast: schema errors are surfaced during startup instead of being silently ignored. Re-running migration is idempotent for the built-in schema and indexes.

## Persistent Storage (Redis)

> **Experimental:** `RedisJobStore` is fully implemented and covered by integration tests, but it is currently marked as **experimental for production workloads**. Please validate it under your own Redis persistence, eviction, failover, and load settings before relying on it for critical scheduling.

Redis can be used as a full `JobStore` when low-latency distributed coordination is preferred over JDBC. The Redis store persists job definitions and executions, uses sorted sets for bounded polling and lease recovery, and uses Lua scripts for atomic claim and replacement transitions.

```kotlin
import io.khrona.store.redis.RedisJobStore
import io.khrona.store.redis.RedisJobStoreConfig
import java.time.Duration

val store = RedisJobStore(
    RedisJobStoreConfig(
        redisUri = "redis://localhost:6379",
        namespace = "khrona-prod",
        commandTimeout = Duration.ofSeconds(2),
        autoReconnect = true,
        requestQueueSize = 10_000
    )
)

install(Khrona) {
    this.store = store
}
```

### Local Redis for Development

Start Redis with the included scripts. It uses settings that match Khrona's scheduler durability expectations: append-only persistence, periodic snapshots, `noeviction`, and a local development password.

```bash
./docker-start.sh
```

Use this URI with the container above:

```kotlin
val store = RedisJobStore(
    RedisJobStoreConfig(
        redisUri = "redis://:khrona_dev_password@localhost:6379/0",
        namespace = "khrona-local",
        commandTimeout = Duration.ofSeconds(2),
        requestQueueSize = 1_000
    )
)
```

Check logs or stop the container:

```bash
docker compose logs redis
./docker-stop.sh
```

For a full cleanup (removing volumes and data):

```bash
docker compose down -v
```

For a no-password quick smoke test, remove `--requirepass` from `docker-compose.yml` and use `redis://localhost:6379/0`. Do not use that setup for shared environments.

Use Redis URI features for security and topology:

- AUTH / ACL: `redis://username:password@redis.example.com:6379`
- TLS: `rediss://redis.example.com:6380`
- Database selection: `redis://localhost:6379/1`

Operational requirements:

- Enable Redis persistence (`AOF` or appropriate `RDB`) if scheduled state must survive Redis restarts.
- Do not use an eviction policy that can evict Khrona keys. Prefer dedicated Redis memory or `noeviction` for scheduler data.
- Keep `namespace` unique per environment or tenant to avoid cross-application key collisions.
- Use `pollBatchSize` on `KhronaConfig` and `requestQueueSize` on `RedisJobStoreConfig` as the scheduler and client-side backpressure boundaries.
- v0.4 does not include automatic terminal-execution cleanup. Use operator-owned cleanup scripts for old `SUCCESS`, `FAILED`, `MISFIRED`, and `SUPERSEDED` records after your retention window. Do not delete `PENDING`, `CLAIMED`, `RUNNING`, or `DEAD_LETTERED` executions unless you intentionally want to remove work or investigation data.

## MySQL 8 & Multi-Node Testing

Khrona is designed to scale across multiple instances. Use a persistent store to coordinate work.

### Local Multi-Instance Setup
To test distributed locking and claiming locally, start a MySQL container:

```bash
docker run -d --name khrona-mysql \
  -e MYSQL_DATABASE=khrona_db \
  -e MYSQL_USER=khrona_user \
  -e MYSQL_PASSWORD=khrona_password \
  -e MYSQL_ROOT_PASSWORD=root \
  -p 3306:3306 mysql:8.0
```

To stop and clean up:
```bash
docker stop khrona-mysql && docker rm khrona-mysql
```

Run two instances of your app in different terminals:
```bash
# Terminal 1
NODE_NAME=node-1 ./gradlew run

# Terminal 2
NODE_NAME=node-2 ./gradlew run
```
Only one node will claim and execute the job at a time if `concurrencyPolicy = ConcurrencyPolicy.FORBID` is used.

## Advanced Configuration

### Retry Policies
Handle failures gracefully with configurable exponential backoff.

```kotlin
job("payment-sync") {
    every(1.hours)
    retry {
        maxAttempts = 5
        initialDelay = 10.seconds
        maxDelay = 1.hours
        factor = 2.0 // Exponential growth
        jitter = 0.1 // 10% randomization
    }
    execute {
        // Sync logic that might fail
    }
}
```

### Concurrency & Locking
Prevent a job from running if a previous execution is still active globally. `lockKey` defaults to the job ID.

```kotlin
job("heavy-task") {
    every(5.minutes)
    // FORBID: Skip the new run if the old one is still active
    // REPLACE: Claim the new run, then supersede and cancel older active runs for the same lock
    concurrencyPolicy = ConcurrencyPolicy.REPLACE 
    
    timeout = 15.minutes // Enforced via coroutine withTimeout
    
    execute {
        // Safe, bound execution with automatic cancellation support
    }
}
```

### Long-Running Jobs, Timeouts, and Restarts
`timeout` is optional. If a job does not set a timeout, Khrona does not enforce a per-execution runtime limit while the application process is alive. The handler can keep running until it completes, fails, the scheduler is stopped, or the coroutine is cancelled by the host application.

For long-running jobs, set a timeout slightly above the expected maximum runtime so a stuck handler does not stay `RUNNING` forever:

```kotlin
job("long-report") {
    once()
    timeout = java.time.Duration.ofHours(21)

    execute {
        // Expected to complete within about 20 hours
    }
}
```

Persistent stores such as `JdbcJobStore` keep execution state across application restarts. If the application crashes or restarts while a job is `CLAIMED` or `RUNNING`, heartbeats stop, the execution lease eventually expires, and the scheduler can pick the execution up again after restart. This starts the handler again from the beginning; Khrona does not resume from the last line of code that ran.

Design long-running handlers to be idempotent, checkpoint progress externally, or split the work into smaller executions. This avoids duplicate side effects when a restarted or reclaimed execution runs again.

### Graceful Shutdown and Cancellation
`scheduler.stop()` is a suspend function that performs a graceful shutdown:

1. It cancels the scheduler polling loop so no new executions are claimed.
2. It waits for currently active job handlers to finish.
3. If active handlers do not finish before `shutdownTimeout`, it cancels the remaining job coroutines.
4. It releases any still-active executions owned by this scheduler from `CLAIMED` or `RUNNING` back to `PENDING`, so another scheduler can pick them up later.

`shutdownTimeout` defaults to 25 seconds and can be configured on the scheduler:

```kotlin
val config = Khrona {
    store = MemoryJobStore()
    shutdownTimeout(20.seconds)
}
```

If you run Khrona in Kubernetes, keep `shutdownTimeout` lower than the pod's `terminationGracePeriodSeconds`. For example, with Kubernetes' common 30 second grace period, a 20-25 second Khrona shutdown timeout leaves time for cancellation handlers, status updates, connection cleanup, and process exit before the container is killed.

Cancellation is cooperative. Timeouts, `ConcurrencyPolicy.REPLACE`, heartbeat loss, and `scheduler.stop()` timeout cancellation all use coroutine cancellation. A manually triggered execution can also cause an older active execution to be cancelled when the job uses `ConcurrencyPolicy.REPLACE` and the new execution is claimed. Handlers that call cancellable suspending APIs such as `delay`, Ktor HTTP clients, or database drivers with coroutine support will receive `CancellationException` and can clean up in `try/finally` or `catch` blocks:

```kotlin
job("sync-report") {
    every(15.minutes)
    timeout = 10.minutes

    execute {
        try {
            runReportSync()
        } finally {
            closeTemporaryResources()
        }
    }
}
```

Blocking or non-cooperative code is not forcibly interrupted by coroutine cancellation. For long-running handlers, prefer cancellable suspending APIs. For CPU-bound or loop-based handlers, call `kotlinx.coroutines.ensureActive()` or `yield()` at safe points so cancellation can be observed promptly. Keep side effects idempotent so a released or timed-out execution can be retried safely.

### Structured Payloads
Khrona preserves JSON-compatible payload structure when using persistent storage (JDBC). Maps, Lists, strings, numbers, booleans, and nested combinations round-trip through `payload_json`.

Malformed persisted payload JSON fails fast when an execution is loaded, so corrupt or manually edited payload data is surfaced at the store boundary instead of being passed to job handlers as a raw string.

> For custom classes, pass an explicit JSON-compatible representation today. Payload schema/version helpers are planned as a future hardening area.

```kotlin
// Triggering with a complex payload
scheduler.trigger("report-job", payload = mapOf(
    "id" to 123,
    "filters" to listOf("ACTIVE", "PENDING")
))

// Inside the job handler, the structure is preserved
execute { payload ->
    val data = payload as Map<*, *>
    val id = data["id"] as Long
}
```

### Misfire Policies
Define what happens if a job misses its scheduled time (e.g., due to downtime).

`MisfirePolicy.FIRE_NOW` is the default. When an execution is older than `misfireThreshold`, Khrona runs that delayed execution as soon as it is claimed:

```kotlin
job("report") {
    cron("0 0 * * *")
    // misfirePolicy defaults to MisfirePolicy.FIRE_NOW
    execute { ... }
}
```

Misfire policies apply to persisted executions that already exist in the store. They do not make a newly registered cron job catch up earlier schedule times. For example, if a job with `cron("54 10 * * *")` is first registered at 22:54 UTC, Khrona schedules the next 10:54 UTC occurrence, usually tomorrow. It does not fire immediately unless an execution for today at 10:54 UTC was already persisted and is now late.

Set `MisfirePolicy.IGNORE` when missed executions should be skipped instead:

```kotlin
job("report") {
    cron("0 0 * * *")
    misfirePolicy = MisfirePolicy.IGNORE // Skip if more than 60s late
    execute { ... }
}
```

For cron jobs, `IGNORE` marks the delayed execution as `MISFIRED` and schedules the next cron occurrence after the current time. If the cron trigger has a timezone, that next occurrence is calculated using the configured timezone and then stored as a UTC `Instant`.

### Correlation ID & Observability
Khrona automatically manages `correlationId` propagation via Slf4j MDC, making it easy to trace a specific job execution across your logs.

- **Uniqueness**: Every new recurring run or manual trigger gets a unique `correlationId`.
- **Retries**: Retries of a failing execution **share the same ID**, allowing you to trace the entire lifecycle of a specific attempt.
- **Propagation**: If you register or trigger a job from a context that already has a `correlationId` in the MDC (like a Ktor request), Khrona will capture and use it.

To show the ID in your logs, update your `logback.xml` pattern to include `%X{correlationId}`:

```xml
<pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger - [%X{correlationId}] %msg%n</pattern>
```

## Manual Control (Standalone)

You can also run Khrona outside of Ktor. Note that registration and triggering are **suspend** functions for better error handling and observability.

```kotlin
val config = Khrona {
    store = MemoryJobStore()
    pollingInterval(5.seconds)
}

val scheduler = Scheduler(config)
scheduler.start()

runBlocking {
    // Register jobs dynamically (suspend function)
    scheduler.registerJob(job("one-time-task") {
        once()
        execute { println("Hello!") }
    })

    // Manually trigger an existing job (suspend function)
    scheduler.trigger("one-time-task", payload = "Ad-hoc data")

    // Stop cleanly
    scheduler.stop()
}
```

## Trigger Formats

### Cron Trigger (Unix Format)
Khrona uses the standard **Unix 5-field format**: `min hour dom month dow`.

`cron(expression)` is evaluated in **UTC** by default:

```kotlin
job("daily-report") {
    cron("0 9 * * *")
    execute { ... }
}
```

Pass a `ZoneId` when the cron expression should follow a local wall-clock schedule:

```kotlin
import java.time.ZoneId

job("weekday-report") {
    cron("0 9 * * 1-5", ZoneId.of("America/New_York"))
    execute { ... }
}
```

- `* * * * *` : Every minute
- `0 * * * *` : Every hour
- `30 5 * * 1` : Every Monday at 05:30 UTC
- `0 9 * * 1-5` with `ZoneId.of("America/New_York")` : Every weekday at 09:00 New York local time

Khrona still stores and compares scheduled executions as UTC `Instant`s. The timezone only controls how cron wall-clock fields are interpreted before the next run is converted back to an `Instant`. Around daylight saving changes, schedules follow the local wall-clock rules for the configured zone; skipped or repeated local times use cron-utils' `ZonedDateTime` behavior.

### Interval Trigger
Use Kotlin's `Duration` for human-readable intervals:
- `every(30.seconds)`
- `every(1.hours)`

## Roadmap

- [x] **v0.1:** Core engine, Kotlin DSL, Ktor integration.
- [x] **v0.2:** Persistence support (Postgres, H2), retries, and dead-letter handling.
- [x] **v0.3:** Distributed execution, lease-based claiming, and concurrency policies.
- [x] **v0.3.3:** Reliability hardening (Registry, Timeouts, Atomic REPLACE).
- [x] **v0.4:** Redis-backed `JobStore` with atomic claiming, heartbeat, recovery, and lock semantics. **(Experimental for production use)**
- [ ] **v0.5:** Admin UI & Dashboard, Metrics (Micrometer/OpenTelemetry), lock inspection.
- [ ] **v0.6:** Production hardening, testkit improvements, adaptive delay, and operator ergonomics.
- [ ] **v0.7:** Android SQLite storage support.

## License

Apache License 2.0
