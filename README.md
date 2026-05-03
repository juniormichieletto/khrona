# Khrona

**Khrona** is a coroutine-native job scheduling and background execution runtime designed for Kotlin and deeply integrated with Ktor.

It provides a reliable, idiomatic, and production-capable platform for background tasks, ranging from simple recurring jobs to distributed-safe persistent executions.

> **Etymology:** The name **Khrona** is inspired by **Chronos** (Ancient Greek: χρόνος, “time”), the personification of time and the root of modern terms like “chronology.” The stylized spelling reflects a modern, system-oriented interpretation of temporal control and scheduling.

## Features

- **🚀 Coroutine-native:** Built on Kotlin coroutines for efficient, non-blocking execution.
- **🛠️ Fluent DSL:** Define jobs and schedules using a clean, Kotlin-first DSL.
- **⏱️ Flexible Triggers:**
  - **Interval:** Run jobs at fixed durations (e.g., every 5 minutes).
  - **Cron:** Standard Unix-format cron expressions (`* * * * *`).
- **🛡️ Reliability & Resilience:**
  - **Retry Policies:** Configurable exponential backoff with jitter.
  - **Concurrency Control:** Manage overlapping executions with `FORBID` or `REPLACE` policies.
  - **Misfire Policies:** Handle delayed executions (FIRE_NOW or IGNORE).
- **💾 Persistence Support:** Durable job storage using JDBC (PostgreSQL, MySQL, Oracle, H2).
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
        
        subgraph "JDBC Dialects"
            JDBC --- Postgres[PostgreSQL]
            JDBC --- MySql[MySQL 8]
            JDBC --- Oracle[Oracle]
            JDBC --- H2[H2]
        end
    end

    DSL --> Sch
    Sch <--> Store
    Workers <--> Store
```

> For a detailed breakdown of the execution flow and sequence diagrams, see the [Full Architecture Document](.specs/codebase/ARCHITECTURE.md).

## Installation

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.github.juniormichieletto:khrona-ktor:0.3.2")
    
    // Choose your storage:
    implementation("io.github.juniormichieletto:khrona-store-memory:0.3.2") // For dev/testing
    implementation("io.github.juniormichieletto:khrona-store-jdbc:0.3.2")   // For production
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

```kotlin
job("report") {
    cron("0 0 * * *")
    misfirePolicy = MisfirePolicy.IGNORE // Skip if more than 60s late
    execute { ... }
}
```

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

> **Note:** All cron expressions are evaluated in **UTC** time.

- `* * * * *` : Every minute
- `0 * * * *` : Every hour
- `30 5 * * 1` : Every Monday at 05:30 UTC

### Interval Trigger
Use Kotlin's `Duration` for human-readable intervals:
- `every(30.seconds)`
- `every(1.hours)`

## Roadmap

- [x] **v0.1:** Core engine, Kotlin DSL, Ktor integration.
- [x] **v0.2:** Persistence support (Postgres, H2), retries, and dead-letter handling.
- [x] **v0.3:** Distributed execution, lease-based claiming, and concurrency policies.
- [x] **v0.3.2:** Reliability hardening (Registry, Timeouts, Atomic REPLACE).
- [ ] **v0.4:** Admin UI & Dashboard, Metrics (Micrometer).
- [ ] **v0.5:** SQLite storage support (Android friendly), Redis-backed distributed locking.
- [ ] **v0.6:** Performance tuning and advanced misfire policies.

## License

Apache License 2.0
