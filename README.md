# Khrona

**Khrona** is a coroutine-native job scheduling and background execution runtime designed for Kotlin and deeply integrated with Ktor.

It provides a reliable, idiomatic, and production-capable platform for background tasks, ranging from simple recurring jobs to distributed-safe persistent executions.

## Features

- **🚀 Coroutine-native:** Built on Kotlin coroutines for efficient, non-blocking execution.
- **🛠️ Fluent DSL:** Define jobs and schedules using a clean, Kotlin-first DSL.
- **⏱️ Flexible Triggers:**
  - **Interval:** Run jobs at fixed durations (e.g., every 5 minutes).
  - **Cron:** Standard Unix-format cron expressions (`* * * * *`).
- **🛡️ Reliability & Resilience:**
  - **Retry Policies:** Configurable exponential backoff with jitter.
  - **Concurrency Control:** Manage overlapping executions with `FORBID` or `REPLACE` policies.
  - **Timeouts:** Ensure jobs don't run longer than expected.
- **💾 Persistence Support:** Durable job storage using JDBC (PostgreSQL, MySQL, Oracle, H2).
- **🌐 Distributed Ready:** Multi-node deployment support with lease-based claiming and distributed locking.
- **🔌 Ktor Integration:** First-class Ktor plugin with seamless lifecycle management.

## Getting Started

### Installation

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.khrona:khrona-ktor:0.3.0")
    implementation("io.khrona:khrona-store-jdbc:0.3.0") // For persistent storage
}
```

### Basic Usage with Ktor

```kotlin
import io.khrona.ktor.*
import io.khrona.store.jdbc.JdbcJobStore
import kotlin.time.Duration.Companion.minutes

fun Application.module() {
    install(Khrona) {
        // Use JDBC for persistent jobs across restarts
        store = JdbcJobStore(dataSource)
    }

    scheduler {
        job("cleanup-temp-files") {
            description = "Deletes temporary upload files every 10 minutes"
            every(10.minutes)
            execute {
                println("Cleaning up...")
            }
        }

        job("daily-report") {
            cron("0 0 * * *") // Every day at midnight UTC (Unix format)
            execute {
                println("Generating daily report...")
            }
        }
    }
}
```

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
Prevent a job from running if a previous execution is still active, even across different nodes.

```kotlin
job("heavy-task") {
    every(5.minutes)
    concurrencyPolicy = ConcurrencyPolicy.FORBID
    lockKey = "global-heavy-task-lock" // Distributed lock key
    timeout = Duration.ofMinutes(15)   // Max execution time
    
    execute {
        // Only one instance of this job runs at a time globally
    }
}
```

### Persistence (JDBC)
Khrona supports multiple databases via `JdbcJobStore`. It handles automatic schema migration and worker claiming.

```kotlin
val store = JdbcJobStore(dataSource).apply { migrate() }

install(Khrona) {
    this.store = store
    this.pollingInterval = 5.seconds.toJavaDuration() // How often to check for jobs
}
```

## Manual Control
You can also run Khrona outside of Ktor:

```kotlin
val config = Khrona {
    store = MemoryJobStore()
    pollingInterval = 1.seconds.toJavaDuration()
}

val scheduler = Scheduler(config)
scheduler.start()

// Register jobs dynamically
scheduler.registerJob(myJobDefinition)

// Stop cleanly
scheduler.stop()
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
- [ ] **v0.4:** Admin UI & Dashboard, Metrics (Micrometer).
- [ ] **v0.5:** Performance tuning and advanced misfire policies.

## License

Apache License 2.0
