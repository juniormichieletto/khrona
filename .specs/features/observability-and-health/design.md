# Design: Observability and Health (`OBS-1`)

## 1. Core Observability SPI (`khrona-core`)

Create `khrona-core/src/main/kotlin/io/khrona/core/Observability.kt` with zero external telemetry dependencies.

### 1.1 Event Enums & Failure Diagnostics

```kotlin
enum class ScheduleReason {
    INITIAL_SEED,
    RECURRING_NEXT,
    RETRY,
    MANUAL_TRIGGER
}

enum class ExecutionOutcome {
    SUCCESS,
    FAILED_RETRY,
    DEAD_LETTERED,
    MISFIRED,
    SUPERSEDED,
    CANCELLED,
    SHUTDOWN_CHECKPOINT
}

enum class HeartbeatFailureReason {
    LOST_OWNERSHIP_OR_EXPIRED,
    STORE_ERROR,
    LEASE_BUDGET_EXCEEDED
}

fun interface ErrorDiagnosticFormatter {
    fun format(error: Throwable): String

    companion object {
        const val DEFAULT_MAX_LENGTH = 512

        val DEFAULT = ErrorDiagnosticFormatter { error ->
            val type = error::class.simpleName ?: error::class.java.simpleName
            val rawMessage = error.message?.trim()
            val formatted = if (rawMessage.isNullOrEmpty()) type else "$type: $rawMessage"
            if (formatted.length <= DEFAULT_MAX_LENGTH) formatted
            else formatted.take(DEFAULT_MAX_LENGTH - 3) + "..."
        }
    }
}
```

`Scheduler` uses `config.errorFormatter` whenever persisting `error` on `FAILED`, `DEAD_LETTERED`, or `onExecutionCompleted` diagnostics (e.g., `IllegalStateException` with `null` message persists `"IllegalStateException"` instead of `null`). Host applications can plug in a custom `ErrorDiagnosticFormatter` to redact tokens, emails, or account IDs before persistence and telemetry emission.

### 1.2 `KhronaObserver` Interface & `CompositeKhronaObserver`

All methods have default no-op implementations so listeners only override what they need:

```kotlin
interface KhronaObserver {
    suspend fun <T> aroundExecution(
        context: ExecutionContext,
        block: suspend () -> T
    ): T = block()

    fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {}
    fun onExecutionStarted(context: ExecutionContext, lag: Duration) {}
    fun onExecutionCompleted(
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        duration: Duration,
        errorType: String? = null
    ) {}
    fun onRetryScheduled(context: ExecutionContext, nextAttempt: Int, delay: Duration, errorType: String) {}
    fun onHeartbeatSucceeded(context: ExecutionContext) {}
    fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {}
    fun onStaleExecutionsRecovered(count: Int) {}
    fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {}
    fun onDeadLetterCountSampled(count: Long) {}
    fun onRetentionCleanup(deletedCount: Int) {}
    fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {}
    fun onStoreOperation(operation: String, duration: Duration, success: Boolean, errorType: String? = null) {}

    companion object {
        val NO_OP: KhronaObserver = object : KhronaObserver {}
    }
}
```

`CompositeKhronaObserver(observers: List<KhronaObserver>)` chains `aroundExecution` cleanly in order (using `foldRight`) and invokes each callback inside an isolated `try/catch` that logs observer failures without aborting sibling observers or scheduler execution. `KhronaConfig` adds:

```kotlin
var observer: KhronaObserver = KhronaObserver.NO_OP
var errorFormatter: ErrorDiagnosticFormatter = ErrorDiagnosticFormatter.DEFAULT
var healthPollFailureThreshold: Int = 2
var deadLetterSamplingInterval: Duration = Duration.ofMinutes(1)
```

In `Scheduler.executeJob`, `aroundExecution(executionContext)` wraps **only** the handler invocation (after `RUNNING` is persisted and before completion status persistence), and `CancellationException` thrown by the wrapped `block` is rethrown directly without being swallowed by observer error isolation. Observer callback invocations around scheduling, lag, completion, retries, heartbeats, recovery, lock contention, cleanup, and shutdown run inside an internal `safeNotify` helper (`try { ... } catch (e: CancellationException) { throw e } catch (e: Exception) { log.warn(...) }`).

## 2. Health & Store Counting Contract

Create `khrona-core/src/main/kotlin/io/khrona/core/Health.kt`:

```kotlin
enum class HealthStatus {
    STOPPED,
    STARTING,
    UP,
    DEGRADED,
    DOWN
}

data class StoreHealth(
    val status: HealthStatus,
    val latency: Duration,
    val errorType: String? = null,
    val detail: String? = null
)

data class SchedulerHealth(
    val status: HealthStatus,
    val storeHealth: StoreHealth,
    val workerId: String,
    val activeExecutions: Int,
    val lastPollAt: Instant?,
    val lastSuccessfulPollAt: Instant?,
    val consecutivePollFailures: Int,
    val lastPollErrorType: String? = null,
    val stopping: Boolean
) {
    val isHealthy: Boolean
        get() = status == HealthStatus.UP || status == HealthStatus.DEGRADED

    val isReady: Boolean
        get() = status == HealthStatus.UP
}
```

### 2.1 `JobStore` Additions

Extend `JobStore` with two methods (with safe default fallbacks for minimal custom test stores, and full implementations in `MemoryJobStore`, `JdbcJobStore`, `RedisJobStore`, and `MockJobStore`):

```kotlin
suspend fun checkHealth(): StoreHealth {
    val startNanos = System.nanoTime()
    return try {
        listJobs()
        StoreHealth(
            status = HealthStatus.UP,
            latency = Duration.ofNanos(System.nanoTime() - startNanos)
        )
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        StoreHealth(
            status = HealthStatus.DOWN,
            latency = Duration.ofNanos(System.nanoTime() - startNanos),
            errorType = e::class.simpleName ?: e::class.java.simpleName,
            detail = e.message
        )
    }
}

suspend fun countExecutions(query: JobExecutionQuery = JobExecutionQuery()): Long
```

Store implementations:
- **`MemoryJobStore` / `MockJobStore`:**
  - `checkHealth()` returns `HealthStatus.UP` with elapsed nanos (unless overridden in tests).
  - `countExecutions(query)`: filters `executions.values.asSequence().filter(query::matches).count().toLong()` (ignoring `query.limit` so DLQ gauge counts the full matching set rather than capping at `100`).
- **`JdbcJobStore` / `JdbcDialect`:**
  - `checkHealth()` executes a fast validation query (`SELECT 1` for Postgres/MySQL/H2, `SELECT 1 FROM DUAL` for Oracle via `JdbcDialect.healthCheckSql()`), measuring latency and catching `Exception` -> `HealthStatus.DOWN` with `errorType`.
  - `countExecutions(query)`: executes `SELECT COUNT(*) FROM khrona_executions [WHERE ...]` via `JdbcDialect.countExecutionsSql(...)` reusing the shared `WHERE` builder from `listExecutionsSql` (never transferring rows or deserializing `payload_json`).
- **`RedisJobStore`:**
  - `checkHealth()` runs `commands.ping()` inside `inRedisContext` and verifies `"PONG"`, returning `HealthStatus.DOWN` on `Exception` (including `RedisException` / `KhronaRedisOomException`).
  - `countExecutions(query)`: scans `keys.executions` via `HSCAN` with a `HashSet<String>` of seen execution IDs, decoding status/jobId/scheduledAt and counting matches (ignoring `query.limit`).

### 2.2 `Scheduler.health()` State Machine

`Scheduler` tracks loop timestamps and poll failures with atomic/volatile state:
- Before `start()` (or after `stop()`): `status = HealthStatus.STOPPED` (`isReady == false`).
- Immediately after `start()` is called until the first poll finishes: `status = HealthStatus.STARTING`.
- If `stopping == true`: `status = HealthStatus.STOPPED`.
- When running:
  - Calls `store.checkHealth()`.
  - If `storeHealth.status == HealthStatus.DOWN` or `consecutivePollFailures >= config.healthPollFailureThreshold`: `status = HealthStatus.DOWN`.
  - Else if `consecutivePollFailures > 0` (single transient poll glitch while store ping succeeds) or poll loop age exceeds `max(pollingInterval * 3, 5.seconds)`: `status = HealthStatus.DEGRADED`.
  - Else: `status = HealthStatus.UP`.
- Upon recovery (the next poll cycle succeeds), `consecutivePollFailures` resets to `0`, `lastSuccessfulPollAt` updates, and `Scheduler.health()` transitions back to `HealthStatus.UP`.

### 2.3 Dead-Letter Size Sampling

Inside the main scheduler loop (cadenced by `config.deadLetterSamplingInterval`, default 1 minute, matching stale recovery):
- Queries `store.countExecutions(JobExecutionQuery(statuses = setOf(ExecutionStatus.DEAD_LETTERED)))` and emits `observer.onDeadLetterCountSampled(count)`.
- Keeps DLQ observation decoupled from every 1-second poll cycle while still tracking DLQ growth and cleanup accurately.

## 3. Optional `khrona-observability` Module

### 3.1 Gradle Configuration

- Include `"khrona-observability"` in `settings.gradle.kts`.
- Add Micrometer (`io.micrometer:micrometer-core:1.14.5`) and OpenTelemetry (`io.opentelemetry:opentelemetry-api:1.47.0`, `io.opentelemetry:opentelemetry-extension-kotlin:1.47.0`, `io.opentelemetry:opentelemetry-sdk-testing:1.47.0` for tests) to `gradle/libs.versions.toml`.
- `khrona-observability/build.gradle.kts` declares:
  - `api(project(":khrona-core"))`
  - `compileOnly(libs.micrometer.core)`
  - `compileOnly(libs.opentelemetry.api)`
  - `compileOnly(libs.opentelemetry.extension.kotlin)`
  - `testImplementation` for Micrometer (`SimpleMeterRegistry`), OpenTelemetry (`OpenTelemetrySdk`, `InMemorySpanExporter`, `InMemoryMetricReader`), JUnit 5, and `kotlinx-coroutines-test`.
- Update `build.gradle.kts` (Pitest subproject set), `.github/scripts/check_coverage.py`, `.github/scripts/check_mutation_coverage.py`, and `.github/workflows/ci.yml` (if needed) to include `khrona-observability` under the same `>= 90%` coverage and mutation gates.

### 3.2 `MicrometerKhronaObserver`

Catalog of meters (configurable prefix defaulting to `"khrona"`, bounded tags only):

| Metric | Type | Bounded Tags |
| --- | --- | --- |
| `khrona.execution.lag` | `Timer` | `job_id` |
| `khrona.execution.duration` | `Timer` | `job_id`, `outcome` |
| `khrona.executions.started` | `Counter` | `job_id` |
| `khrona.executions.scheduled` | `Counter` | `job_id`, `reason` |
| `khrona.executions.completed` | `Counter` | `job_id`, `outcome`, `error_type` |
| `khrona.executions.retried` | `Counter` | `job_id`, `error_type` |
| `khrona.heartbeat.total` | `Counter` | `job_id`, `status` (`succeeded` / `failed`), `reason` |
| `khrona.recovery.stale` | `Counter` | *(none)* |
| `khrona.lock.contention` | `Counter` | `job_id`, `policy` |
| `khrona.dead_letter.size` | `Gauge` | *(none)* |
| `khrona.retention.deleted` | `Counter` | *(none)* |
| `khrona.shutdown.duration` | `Timer` | `clean` (`true` / `false`) |
| `khrona.store.operation.duration` | `Timer` | `operation`, `status` (`success` / `error`), `error_type` |

No tag ever includes `executionId`, `correlationId`, `workerId`, `lockKey` (when dynamic), or raw exception message text.

### 3.3 `OpenTelemetryKhronaObserver`

- Implements `aroundExecution(context, block)` using `Tracer.spanBuilder("khrona.job.execute")` (`SpanKind.INTERNAL`), attaching:
  - `khrona.job.id` = `context.jobId`
  - `khrona.execution.id` = `context.executionId.toString()`
  - `khrona.attempt` = `context.attempt.toLong()`
  - `khrona.scheduled_at` = `context.scheduledAt.toString()`
  - `khrona.correlation_id` = `context.correlationId`
  - Propagates active OTel context into the suspended coroutine via `Context.current().with(span).asContextElement()`, records exceptions with `span.recordException(t)` and `StatusCode.ERROR` on non-checkpoint failures, and ends the span in `finally`.
- Emits bounded-attribute OpenTelemetry metrics (`Meter`) mirroring the Micrometer signal catalog (`LongCounter`, `DoubleHistogram`, `ObservableLongGauge` for `khrona.dead_letter.size`).

## 4. Store Latency Decoration (Optional Helper)

Provide `ObservableJobStore(delegate: JobStore, observer: KhronaObserver): JobStore` (or `JobStore.observedBy(observer)` helper) that measures store operation wall time and emits `observer.onStoreOperation(operation, duration, success, errorType)` cleanly without forcing latency overhead on stores that do not wrap calls.

## 5. Ktor E2E & Documentation Scope

- Extend `KtorGeneratorDockerE2ETest` with generated-app verification of:
  - `MicrometerKhronaObserver` recording successful execution, retry, dead-letter, and bounded tag assertions.
  - Host-owned `/health` route backed by `scheduler.health()` transitioning `UP` -> `DOWN` during a controlled store outage (e.g., proxy/fault-injected `JobStore` wrapper or paused DB) -> `UP` upon recovery.
- Document in `README.md`:
  - `KhronaObserver`, `MicrometerKhronaObserver`, `OpenTelemetryKhronaObserver`, and `Scheduler.health()`.
  - Host-owned `/health`, `/ready`, and `/metrics` Ktor integration examples.
  - Sensitive data / redaction guidance (`ErrorDiagnosticFormatter` and payload/exception hygiene).
