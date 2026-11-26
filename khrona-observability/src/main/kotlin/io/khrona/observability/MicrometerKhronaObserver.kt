package io.khrona.observability

import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.ExecutionContext
import io.khrona.core.ExecutionOutcome
import io.khrona.core.HeartbeatFailureReason
import io.khrona.core.KhronaObserver
import io.khrona.core.ScheduleReason
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * [KhronaObserver] adapter emitting bounded-cardinality Micrometer counters, timers, and gauges
 * into a host-managed [MeterRegistry].
 */
class MicrometerKhronaObserver(
    private val registry: MeterRegistry,
    metricPrefix: String = DEFAULT_METRIC_PREFIX
) : KhronaObserver {

    private val prefix: String = metricPrefix.trim().also {
        require(it.isNotEmpty()) { "metricPrefix must not be blank" }
    }
    private val deadLetterSize = AtomicLong(0L)

    init {
        Gauge.builder("$prefix.dead_letter.size", deadLetterSize) { it.get().toDouble() }
            .description("Current number of dead-lettered job executions")
            .register(registry)
    }

    override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {
        Counter.builder("$prefix.executions.scheduled")
            .tag("job_id", jobId)
            .tag("reason", reason.name.lowercase())
            .register(registry)
            .increment()
    }

    override fun onExecutionStarted(context: ExecutionContext, lag: Duration) {
        Timer.builder("$prefix.execution.lag")
            .tag("job_id", context.jobId)
            .register(registry)
            .record(clampNonNegative(lag))

        Counter.builder("$prefix.executions.started")
            .tag("job_id", context.jobId)
            .register(registry)
            .increment()
    }

    override fun onExecutionCompleted(
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        duration: Duration,
        errorType: String?
    ) {
        val outcomeTag = outcome.name.lowercase()
        val errorTypeTag = errorType ?: "none"

        Timer.builder("$prefix.execution.duration")
            .tag("job_id", context.jobId)
            .tag("outcome", outcomeTag)
            .register(registry)
            .record(clampNonNegative(duration))

        Counter.builder("$prefix.executions.completed")
            .tag("job_id", context.jobId)
            .tag("outcome", outcomeTag)
            .tag("error_type", errorTypeTag)
            .register(registry)
            .increment()
    }

    override fun onRetryScheduled(
        context: ExecutionContext,
        nextAttempt: Int,
        delay: Duration,
        errorType: String
    ) {
        Counter.builder("$prefix.executions.retried")
            .tag("job_id", context.jobId)
            .tag("error_type", errorType)
            .register(registry)
            .increment()
    }

    override fun onHeartbeatSucceeded(context: ExecutionContext) {
        Counter.builder("$prefix.heartbeat.total")
            .tag("job_id", context.jobId)
            .tag("status", "succeeded")
            .tag("reason", "none")
            .register(registry)
            .increment()
    }

    override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {
        Counter.builder("$prefix.heartbeat.total")
            .tag("job_id", context.jobId)
            .tag("status", "failed")
            .tag("reason", reason.name.lowercase())
            .register(registry)
            .increment()
    }

    override fun onStaleExecutionsRecovered(count: Int) {
        if (count > 0) {
            Counter.builder("$prefix.recovery.stale")
                .description("Total stale executions recovered after lease expiry")
                .register(registry)
                .increment(count.toDouble())
        }
    }

    override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {
        Counter.builder("$prefix.lock.contention")
            .tag("job_id", jobId)
            .tag("policy", policy.name.lowercase())
            .register(registry)
            .increment()
    }

    override fun onDeadLetterCountSampled(count: Long) {
        deadLetterSize.set(count.coerceAtLeast(0L))
    }

    override fun onRetentionCleanup(deletedCount: Int) {
        if (deletedCount > 0) {
            Counter.builder("$prefix.retention.deleted")
                .description("Total completed executions purged by retention cleanup")
                .register(registry)
                .increment(deletedCount.toDouble())
        }
    }

    override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {
        Timer.builder("$prefix.shutdown.duration")
            .tag("clean", clean.toString())
            .register(registry)
            .record(clampNonNegative(duration))
    }

    override fun onStoreOperation(
        operation: String,
        duration: Duration,
        success: Boolean,
        errorType: String?
    ) {
        Timer.builder("$prefix.store.operation.duration")
            .tag("operation", operation)
            .tag("status", if (success) "success" else "error")
            .tag("error_type", errorType ?: "none")
            .register(registry)
            .record(clampNonNegative(duration))
    }

    private fun clampNonNegative(duration: Duration): Duration =
        if (duration.isNegative) Duration.ZERO else duration

    companion object {
        const val DEFAULT_METRIC_PREFIX = "khrona"
    }
}
