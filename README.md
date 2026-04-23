# Khrona

**Khrona** is a coroutine-native job scheduling and background execution runtime designed for Kotlin and deeply integrated with Ktor.

It provides a reliable, idiomatic, and production-capable platform for background tasks, ranging from simple recurring jobs to distributed-safe persistent executions.

## Features

- **Coroutine-native:** Built on Kotlin coroutines for efficient, non-blocking execution.
- **Fluent DSL:** Define jobs and schedules using a clean, Kotlin-first domain-specific language.
- **Multiple Trigger Types:**
  - **Interval:** Run jobs at fixed durations.
  - **Cron:** Schedule jobs using standard cron expressions.
  - **Startup:** Execute tasks once when the application starts (with optional delay).
  - **One-time:** Schedule persistent or ephemeral deferred tasks.
- **Ktor Integration:** Seamlessly installs as a Ktor plugin with lifecycle management.
- **Persistence Support:** (In-progress) Support for durable job storage and recovery across restarts.
- **Distributed Ready:** Designed for multi-node deployments with distributed locking and claiming.
- **Observability:** Built-in support for metrics, tracing, and structured logging.

## Getting Started

### Installation (Coming Soon)

```kotlin
// build.gradle.kts
dependencies {
    implementation("io.khrona:khrona-ktor:0.1.0")
    implementation("io.khrona:khrona-store-memory:0.1.0")
}
```

### Basic Usage

```kotlin
import io.khrona.ktor.*
import io.khrona.store.memory.MemoryJobStore
import kotlin.time.Duration.Companion.minutes

fun Application.module() {
    install(Khrona) {
        store = MemoryJobStore()
    }

    scheduler {
        job("cleanup-temp-files") {
            every(10.minutes)
            execute {
                // Your job logic here
                println("Cleaning up...")
            }
        }
    }
}
```

## Usage & Testing

### 1. Ktor Integration
To use Khrona in your Ktor application, install the plugin and define your jobs:

```kotlin
fun Application.module() {
    install(Khrona) {
        store = MemoryJobStore()

        job("heartbeat") {
            every(1.minutes)
            execute {
                println("Khrona is alive!")
            }
        }
    }
}
```

### 2. Standalone Usage
You can run the scheduler outside of Ktor as well:

```kotlin
val config = Khrona {
    store = MemoryJobStore()
    job("task") {
        every(30.seconds)
        execute { println("Task running") }
    }
}

val scheduler = Scheduler(config)
scheduler.start()
```

### 3. Verifying the Installation
To verify the library is functioning correctly, you can run the included test suite which covers the scheduler loop, job execution, and store logic:

```bash
./gradlew clean test
```

## Project Structure

- `khrona-core`: The core scheduling engine and DSL.
- `khrona-ktor`: Ktor plugin and integration.
- `khrona-store-memory`: In-memory implementation of the job store (for development and testing).

## Roadmap

- **v0.1:** Core engine, Kotlin DSL, Ktor integration, and basic triggers.
- **v0.2:** Persistence support (Postgres), transactional enqueuing, and dead-letter handling.
- **v0.3:** Distributed execution and coordination.

## License

(Include license information here)
