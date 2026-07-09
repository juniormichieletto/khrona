# Polling Interval Performance Analysis

> [!IMPORTANT]
> **Performance Scope Note:** These benchmarks are intended to provide high-level, measurable numbers to verify that the scheduler and storage engines are operating efficiently under baseline conditions. They serve as an architectural "sanity check" rather than an exhaustive stress test or absolute performance limit. Actual production performance will vary based on hardware, network latency, and the complexity of job handlers.

This document records dated performance analysis runs used to evaluate the resource overhead of the Khrona scheduler across various polling intervals and storage engines.

## Methodology

The benchmarks were conducted using a series of automated JUnit tests. For each configuration, the following procedure was followed:

1.  **Environment Setup:** A clean JVM instance was initialized. For persistent stores (PostgreSQL, MySQL), Testcontainers were used to spin up ephemeral Docker instances. Redis tests utilized a local Docker container started via `./docker-start.sh`.
2.  **Job Registration:** 10 recurring jobs were registered with a 5-minute interval to ensure the scheduler loop was active but not processing actual task logic, focusing purely on polling and claim-check overhead.
3.  **Measurement Window:** For each polling interval (100ms, 1s, 10s, 1m), the scheduler ran for 10 seconds.
4.  **Data Sampling:**
    *   **CPU:** Process CPU load was sampled every 500ms using `com.sun.management.OperatingSystemMXBean`.
    *   **Memory:** Peak used memory (Total - Free) was tracked throughout the window.
5.  **Cleanup:** Between each interval test, `System.gc()` was invoked and a 1-second delay was applied to allow the environment to stabilize.

## Results Summary (Storage Comparison)

Tests conducted with 10 recurring jobs over 10-second measurement windows.

### 2026-05-15 Baseline

#### In-Memory / Local
| Polling Interval | Minimal Store (CPU/Mem) | H2 In-Memory (CPU/Mem) |
| :--- | :--- | :--- |
| **100ms** (PT0.1S) | 0.08% / 22 MB | 0.41% / 31 MB |
| **1s** (PT1S) | 0.13% / 8 MB | 0.07% / 16 MB |
| **10s** (PT10S) | 0.03% / 7 MB | 0.08% / 16 MB |
| **1m** (PT1M) | 0.05% / 6 MB | 0.06% / 16 MB |

#### Distributed / Persistent
| Polling Interval | PostgreSQL (CPU/Mem) | MySQL (CPU/Mem) | Redis (CPU/Mem) |
| :--- | :--- | :--- | :--- |
| **100ms** (PT0.1S) | 1.01% / 45 MB | 1.26% / 30 MB | 0.88% / 28 MB |
| **1s** (PT1S) | 0.10% / 19 MB | 0.14% / 19 MB | 0.16% / 16 MB |
| **10s** (PT10S) | 0.08% / 16 MB | 0.11% / 19 MB | 0.13% / 14 MB |
| **1m** (PT1M) | 0.08% / 16 MB | 0.08% / 19 MB | 0.13% / 14 MB |

### 2026-06-16 Follow-Up Run

This run was performed after the `v0.5.0` release setup. The disabled benchmark tests were enabled with a temporary Gradle init script. The first Redis run was discarded because stale `khrona-perf:*` keys caused old jobs to replay; only the clean rerun after clearing that benchmark namespace is recorded below.

#### In-Memory / Local
| Polling Interval | Minimal Store (CPU/Mem) | H2 In-Memory (CPU/Mem) |
| :--- | :--- | :--- |
| **100ms** (PT0.1S) | 0.11% / 22 MB | 0.39% / 31 MB |
| **1s** (PT1S) | 0.09% / 7 MB | 0.08% / 17 MB |
| **10s** (PT10S) | 0.03% / 7 MB | 0.08% / 17 MB |
| **1m** (PT1M) | 0.05% / 6 MB | 0.07% / 16 MB |

#### Distributed / Persistent
| Polling Interval | PostgreSQL (CPU/Mem) | MySQL (CPU/Mem) | Redis (CPU/Mem) |
| :--- | :--- | :--- | :--- |
| **100ms** (PT0.1S) | 1.11% / 45 MB | 1.42% / 28 MB | 1.06% / 29 MB |
| **1s** (PT1S) | 0.11% / 16 MB | 0.14% / 19 MB | 0.19% / 16 MB |
| **10s** (PT10S) | 0.05% / 16 MB | 0.12% / 19 MB | 0.11% / 14 MB |
| **1m** (PT1M) | 0.07% / 16 MB | 0.07% / 19 MB | 0.13% / 13 MB |

## Key Observations

### 1. Database-Backed Overhead
As expected, persistent storage adds a measurable but still very low CPU overhead compared to the minimal no-op store. 
- **PostgreSQL/MySQL** peak at ~1.0-1.3% CPU when polling at 100ms, mostly due to the JDBC connection management and network I/O with the Docker container.
- **Redis** performs slightly better than the SQL databases at the highest polling frequency (0.88% CPU).

### 2. Polling Efficiency
The "elbow" of the performance curve for all databases is at the **1-second** interval. Increasing the polling rate from 1s to 100ms results in a ~10x increase in relative CPU usage (though absolute usage remains low), whereas increasing from 10s to 1s has a negligible impact.

### 3. Memory Stability
Memory usage remains remarkably stable across all persistent stores, typically staying under **20 MB** for intervals of 1s or more. The higher memory peaks (up to 45 MB for PostgreSQL at 100ms) are transient and likely related to more frequent HikariCP connection usage and JSON serialization/deserialization cycles.

## Recommendation
- **For most production workloads:** A polling interval of **1 second** provides an excellent balance between responsiveness and near-zero resource utilization across all storage types.
- **For high-precision needs:** **100ms** is perfectly sustainable but consider the increased load on your database server if scaling to many scheduler instances.

## Reproducing the Tests
The performance tests are preserved as `@Disabled` JUnit tests in the following files:
- **Baseline:** `khrona-core/src/test/kotlin/io/khrona/core/PollingBaselinePerfTest.kt`
- **JDBC (H2, Postgres, MySQL):** `khrona-store-jdbc/src/test/kotlin/io/khrona/store/jdbc/JdbcPollingPerfTest.kt`
- **Redis:** `khrona-store-redis/src/test/kotlin/io/khrona/store/redis/RedisPollingPerfTest.kt`

To run these tests manually:
1. Ensure Docker is running (for Postgres/MySQL/Redis).
2. Start the local Redis via `./docker-start.sh`.
3. Enable the `@Disabled` tests with a temporary Gradle init script:
   ```groovy
   allprojects {
       tasks.withType(Test).configureEach {
           systemProperty "junit.jupiter.conditions.deactivate", "org.junit.jupiter.engine.extension.DisabledCondition"
       }
   }
   ```
4. Run the desired benchmark task with the init script:
   ```bash
   ./gradlew --init-script /tmp/khrona-enable-disabled-tests.gradle :khrona-store-jdbc:test --tests "io.khrona.store.jdbc.JdbcPollingPerfTest" --rerun-tasks --info
   ```
5. Before rerunning the Redis benchmark against a reused local Redis container, clear only the benchmark namespace to avoid replaying stale jobs:
   ```bash
   docker exec khrona-redis redis-cli -a khrona_dev_password DEL khrona-perf:jobs khrona-perf:executions khrona-perf:pending
   ```

## Future Testing
Future benchmarks should focus on:
- **Performance CI Pipeline:** Automating these tests within the CI/CD pipeline to detect significant CPU/Memory degradation over time.
- Scaling to 1,000+ jobs.
- Comparative overhead of different `JobStore` implementations (Redis vs. JDBC) under high concurrency.
- Impact of network latency on distributed lock acquisition times.
