package io.khrona.observability

import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.ErrorDiagnosticFormatter
import io.khrona.core.ExecutionContext
import io.khrona.core.ExecutionOutcome
import io.khrona.core.HeartbeatFailureReason
import io.khrona.core.KhronaObserver
import io.khrona.core.ScheduleReason
import io.khrona.core.ShutdownCheckpointException
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.metrics.LongCounter
import io.opentelemetry.api.metrics.ObservableLongGauge
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * [KhronaObserver] adapter that creates OpenTelemetry spans via [aroundExecution] (propagating
 * trace context through Kotlin coroutines) and records bounded-cardinality OpenTelemetry metrics.
 */
class OpenTelemetryKhronaObserver(
    openTelemetry: OpenTelemetry,
    instrumentationName: String = DEFAULT_INSTRUMENTATION_NAME,
    metricPrefix: String = DEFAULT_METRIC_PREFIX
) : KhronaObserver, AutoCloseable {

    private val trimmedInstrumentationName: String = instrumentationName.trim().also {
        require(it.isNotEmpty()) { "instrumentationName must not be blank" }
    }
    private val prefix: String = metricPrefix.trim().also {
        require(it.isNotEmpty()) { "metricPrefix must not be blank" }
    }

    private val tracer: Tracer = openTelemetry.getTracer(trimmedInstrumentationName)
    private val deadLetterSize = AtomicLong(0L)

    private val executionLagHistogram: DoubleHistogram
    private val executionDurationHistogram: DoubleHistogram
    private val executionsStartedCounter: LongCounter
    private val executionsScheduledCounter: LongCounter
    private val executionsCompletedCounter: LongCounter
    private val executionsRetriedCounter: LongCounter
    private val heartbeatCounter: LongCounter
    private val staleRecoveryCounter: LongCounter
    private val lockContentionCounter: LongCounter
    private val retentionDeletedCounter: LongCounter
    private val shutdownDurationHistogram: DoubleHistogram
    private val storeOperationDurationHistogram: DoubleHistogram
    private val deadLetterGauge: ObservableLongGauge

    init {
        val meter = openTelemetry.getMeter(trimmedInstrumentationName)
        executionLagHistogram = meter.histogramBuilder("$prefix.execution.lag")
            .setDescription("Scheduling lag from scheduledAt until execution start")
            .setUnit("s")
            .build()
        executionDurationHistogram = meter.histogramBuilder("$prefix.execution.duration")
            .setDescription("Handler execution duration")
            .setUnit("s")
            .build()
        executionsStartedCounter = meter.counterBuilder("$prefix.executions.started")
            .setDescription("Total job executions started")
            .build()
        executionsScheduledCounter = meter.counterBuilder("$prefix.executions.scheduled")
            .setDescription("Total job executions scheduled")
            .build()
        executionsCompletedCounter = meter.counterBuilder("$prefix.executions.completed")
            .setDescription("Total job executions reaching an outcome")
            .build()
        executionsRetriedCounter = meter.counterBuilder("$prefix.executions.retried")
            .setDescription("Total retry executions scheduled")
            .build()
        heartbeatCounter = meter.counterBuilder("$prefix.heartbeat.total")
            .setDescription("Total heartbeat renewals and failures")
            .build()
        staleRecoveryCounter = meter.counterBuilder("$prefix.recovery.stale")
            .setDescription("Total stale executions recovered after lease expiry")
            .build()
        lockContentionCounter = meter.counterBuilder("$prefix.lock.contention")
            .setDescription("Total executions skipped or released due to FORBID lock contention")
            .build()
        retentionDeletedCounter = meter.counterBuilder("$prefix.retention.deleted")
            .setDescription("Total completed executions purged by retention cleanup")
            .build()
        shutdownDurationHistogram = meter.histogramBuilder("$prefix.shutdown.duration")
            .setDescription("Scheduler shutdown duration")
            .setUnit("s")
            .build()
        storeOperationDurationHistogram = meter.histogramBuilder("$prefix.store.operation.duration")
            .setDescription("JobStore operation latency")
            .setUnit("s")
            .build()
        deadLetterGauge = meter.gaugeBuilder("$prefix.dead_letter.size")
            .ofLongs()
            .setDescription("Current number of dead-lettered job executions")
            .buildWithCallback { measurement ->
                measurement.record(deadLetterSize.get())
            }
    }

    override suspend fun <T> aroundExecution(
        context: ExecutionContext,
        block: suspend () -> T
    ): T {
        val span = tracer.spanBuilder("$prefix.job.execute")
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(KHRONA_JOB_ID, context.jobId)
            .setAttribute(KHRONA_EXECUTION_ID, context.executionId.toString())
            .setAttribute(KHRONA_ATTEMPT, context.attempt.toLong())
            .setAttribute(KHRONA_SCHEDULED_AT, context.scheduledAt.toString())
            .setAttribute(KHRONA_CORRELATION_ID, context.correlationId)
            .startSpan()

        return try {
            val result = withContext(Context.current().with(span).asContextElement()) {
                block()
            }
            span.setStatus(StatusCode.OK)
            result
        } catch (e: ShutdownCheckpointException) {
            span.addEvent("khrona.shutdown.checkpoint")
            throw e
        } catch (t: Throwable) {
            if (t is CancellationException && t !is TimeoutCancellationException) {
                span.addEvent("khrona.execution.cancelled")
            } else {
                span.recordException(t)
                span.setStatus(StatusCode.ERROR, ErrorDiagnosticFormatter.typeName(t))
            }
            throw t
        } finally {
            span.end()
        }
    }

    override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {
        executionsScheduledCounter.add(
            1L,
            Attributes.of(TAG_JOB_ID, jobId, TAG_REASON, reason.name.lowercase())
        )
    }

    override fun onExecutionStarted(context: ExecutionContext, lag: Duration) {
        val attrs = Attributes.of(TAG_JOB_ID, context.jobId)
        executionLagHistogram.record(toSeconds(lag), attrs)
        executionsStartedCounter.add(1L, attrs)
    }

    override fun onExecutionCompleted(
        context: ExecutionContext,
        outcome: ExecutionOutcome,
        duration: Duration,
        errorType: String?
    ) {
        val outcomeTag = outcome.name.lowercase()
        executionDurationHistogram.record(
            toSeconds(duration),
            Attributes.of(TAG_JOB_ID, context.jobId, TAG_OUTCOME, outcomeTag)
        )
        executionsCompletedCounter.add(
            1L,
            Attributes.of(
                TAG_JOB_ID,
                context.jobId,
                TAG_OUTCOME,
                outcomeTag,
                TAG_ERROR_TYPE,
                errorType ?: "none"
            )
        )
    }

    override fun onRetryScheduled(
        context: ExecutionContext,
        nextAttempt: Int,
        delay: Duration,
        errorType: String
    ) {
        executionsRetriedCounter.add(
            1L,
            Attributes.of(TAG_JOB_ID, context.jobId, TAG_ERROR_TYPE, errorType)
        )
    }

    override fun onHeartbeatSucceeded(context: ExecutionContext) {
        heartbeatCounter.add(
            1L,
            Attributes.of(TAG_JOB_ID, context.jobId, TAG_STATUS, "succeeded", TAG_REASON, "none")
        )
    }

    override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {
        heartbeatCounter.add(
            1L,
            Attributes.of(TAG_JOB_ID, context.jobId, TAG_STATUS, "failed", TAG_REASON, reason.name.lowercase())
        )
    }

    override fun onStaleExecutionsRecovered(count: Int) {
        if (count > 0) {
            staleRecoveryCounter.add(count.toLong())
        }
    }

    override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {
        lockContentionCounter.add(
            1L,
            Attributes.of(TAG_JOB_ID, jobId, TAG_POLICY, policy.name.lowercase())
        )
    }

    override fun onDeadLetterCountSampled(count: Long) {
        deadLetterSize.set(count.coerceAtLeast(0L))
    }

    override fun onRetentionCleanup(deletedCount: Int) {
        if (deletedCount > 0) {
            retentionDeletedCounter.add(deletedCount.toLong())
        }
    }

    override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {
        shutdownDurationHistogram.record(
            toSeconds(duration),
            Attributes.of(TAG_CLEAN, clean.toString())
        )
    }

    override fun onStoreOperation(
        operation: String,
        duration: Duration,
        success: Boolean,
        errorType: String?
    ) {
        storeOperationDurationHistogram.record(
            toSeconds(duration),
            Attributes.of(
                TAG_OPERATION,
                operation,
                TAG_STATUS,
                if (success) "success" else "error",
                TAG_ERROR_TYPE,
                errorType ?: "none"
            )
        )
    }

    override fun close() {
        deadLetterGauge.close()
    }

    private fun toSeconds(duration: Duration): Double {
        if (duration.isNegative) return 0.0
        return duration.toNanos() / NANOS_PER_SECOND
    }

    companion object {
        const val DEFAULT_INSTRUMENTATION_NAME = "io.khrona"
        const val DEFAULT_METRIC_PREFIX = "khrona"
        private const val NANOS_PER_SECOND = 1_000_000_000.0

        private val KHRONA_JOB_ID = AttributeKey.stringKey("khrona.job.id")
        private val KHRONA_EXECUTION_ID = AttributeKey.stringKey("khrona.execution.id")
        private val KHRONA_ATTEMPT = AttributeKey.longKey("khrona.attempt")
        private val KHRONA_SCHEDULED_AT = AttributeKey.stringKey("khrona.scheduled_at")
        private val KHRONA_CORRELATION_ID = AttributeKey.stringKey("khrona.correlation_id")

        private val TAG_JOB_ID = AttributeKey.stringKey("job_id")
        private val TAG_REASON = AttributeKey.stringKey("reason")
        private val TAG_OUTCOME = AttributeKey.stringKey("outcome")
        private val TAG_ERROR_TYPE = AttributeKey.stringKey("error_type")
        private val TAG_STATUS = AttributeKey.stringKey("status")
        private val TAG_POLICY = AttributeKey.stringKey("policy")
        private val TAG_CLEAN = AttributeKey.stringKey("clean")
        private val TAG_OPERATION = AttributeKey.stringKey("operation")
    }
}
