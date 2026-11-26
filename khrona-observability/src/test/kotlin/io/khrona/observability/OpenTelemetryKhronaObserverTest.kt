package io.khrona.observability

import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.ExecutionContext
import io.khrona.core.ExecutionOutcome
import io.khrona.core.HeartbeatFailureReason
import io.khrona.core.ScheduleReason
import io.khrona.core.ShutdownCheckpointException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.Span
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.api.trace.StatusCode
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID

class OpenTelemetryKhronaObserverTest {

    private fun sampleContext(
        executionId: UUID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        jobId: String = "otel-sync",
        attempt: Int = 2,
        correlationId: String = "otel-corr-secret-1"
    ): ExecutionContext = ExecutionContext(
        executionId = executionId,
        jobId = jobId,
        attempt = attempt,
        scheduledAt = Instant.parse("2030-01-01T12:00:00Z"),
        payload = "secret-payload",
        correlationId = correlationId,
        isStopping = { false }
    )

    private fun buildTestOtel(): Triple<OpenTelemetrySdk, InMemorySpanExporter, InMemoryMetricReader> {
        val spanExporter = InMemorySpanExporter.create()
        val tracerProvider = SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
            .build()
        val metricReader = InMemoryMetricReader.create()
        val meterProvider = SdkMeterProvider.builder()
            .registerMetricReader(metricReader)
            .build()
        val sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setMeterProvider(meterProvider)
            .build()
        return Triple(sdk, spanExporter, metricReader)
    }

    @Test
    fun `rejects blank instrumentationName or metricPrefix`() {
        val (sdk, _, _) = buildTestOtel()
        try {
            assertThrows(IllegalArgumentException::class.java) {
                OpenTelemetryKhronaObserver(sdk, instrumentationName = "  ")
            }
            assertThrows(IllegalArgumentException::class.java) {
                OpenTelemetryKhronaObserver(sdk, metricPrefix = "")
            }
        } finally {
            sdk.close()
        }
    }

    @Test
    fun `aroundExecution creates INTERNAL span propagates coroutine context and handles exceptions cleanly`() = runBlocking {
        val (sdk, spanExporter, _) = buildTestOtel()
        val observer = OpenTelemetryKhronaObserver(sdk, instrumentationName = "io.khrona.test", metricPrefix = "custom.otel")
        val ctx = sampleContext()

        try {
            // 1. Success with suspension and child async coroutine inheriting parent OTel span
            var observedChildSpanId: String? = null
            var observedTraceId: String? = null
            val result = observer.aroundExecution(ctx) {
                delay(5)
                val parentSpan = Span.current().spanContext
                observedTraceId = parentSpan.traceId
                observedChildSpanId = kotlinx.coroutines.coroutineScope {
                    async(kotlinx.coroutines.Dispatchers.Default) {
                        delay(5)
                        Span.current().spanContext.spanId
                    }.await()
                }
                "ok-result"
            }
            assertEquals("ok-result", result)
            assertEquals("sync-ok", observer.aroundExecution(ctx) { "sync-ok" })
            assertEquals(2, spanExporter.finishedSpanItems.size)
            val successSpan = spanExporter.finishedSpanItems.first()
            assertEquals("custom.otel.job.execute", successSpan.name)
            assertEquals(SpanKind.INTERNAL, successSpan.kind)
            assertEquals(StatusCode.OK, successSpan.status.statusCode)
            assertEquals(successSpan.spanContext.spanId, observedChildSpanId)
            assertEquals(successSpan.spanContext.traceId, observedTraceId)
            assertEquals("otel-sync", successSpan.attributes.get(AttributeKey.stringKey("khrona.job.id")))
            assertEquals(ctx.executionId.toString(), successSpan.attributes.get(AttributeKey.stringKey("khrona.execution.id")))
            assertEquals(2L, successSpan.attributes.get(AttributeKey.longKey("khrona.attempt")))
            assertEquals("2030-01-01T12:00:00Z", successSpan.attributes.get(AttributeKey.stringKey("khrona.scheduled_at")))
            assertEquals("otel-corr-secret-1", successSpan.attributes.get(AttributeKey.stringKey("khrona.correlation_id")))
            spanExporter.reset()

            // 2. Normal exception records exception event and sets ERROR status
            val boom = IllegalStateException("handler boom")
            val thrown = assertThrows(IllegalStateException::class.java) {
                runBlocking {
                    observer.aroundExecution(ctx) { throw boom }
                }
            }
            assertEquals("handler boom", thrown.message)
            val failedSpan = spanExporter.finishedSpanItems.single()
            assertEquals(StatusCode.ERROR, failedSpan.status.statusCode)
            assertTrue(failedSpan.events.any { it.name == "exception" })
            spanExporter.reset()

            // 3. TimeoutCancellationException records exception and sets ERROR status
            assertThrows(TimeoutCancellationException::class.java) {
                runBlocking {
                    observer.aroundExecution(ctx) {
                        withTimeout(5) { delay(500) }
                    }
                }
            }
            val timeoutSpan = spanExporter.finishedSpanItems.single()
            assertEquals(StatusCode.ERROR, timeoutSpan.status.statusCode)
            assertTrue(timeoutSpan.events.any { it.name == "exception" })
            spanExporter.reset()

            // 4. ShutdownCheckpointException adds event without ERROR status
            assertThrows(ShutdownCheckpointException::class.java) {
                runBlocking {
                    observer.aroundExecution(ctx) { throw ShutdownCheckpointException(ctx.executionId, ctx.jobId) }
                }
            }
            val cpSpan = spanExporter.finishedSpanItems.single()
            assertFalse(cpSpan.status.statusCode == StatusCode.ERROR)
            assertTrue(cpSpan.events.any { it.name == "khrona.shutdown.checkpoint" })
            spanExporter.reset()

            // 5. Cooperative CancellationException adds event without ERROR status
            assertThrows(CancellationException::class.java) {
                runBlocking {
                    observer.aroundExecution(ctx) { throw CancellationException("stopped") }
                }
            }
            val cancelledSpan = spanExporter.finishedSpanItems.single()
            assertFalse(cancelledSpan.status.statusCode == StatusCode.ERROR)
            assertTrue(cancelledSpan.events.any { it.name == "khrona.execution.cancelled" })
        } finally {
            observer.close()
            sdk.close()
        }
    }

    @Test
    fun `emits full bounded OTel metrics catalog without leaking unbounded attributes`() {
        val (sdk, _, metricReader) = buildTestOtel()
        val observer = OpenTelemetryKhronaObserver(sdk)
        val ctx = sampleContext()

        try {
            observer.onStaleExecutionsRecovered(0)
            observer.onRetentionCleanup(0)
            val beforePositive = metricReader.collectAllMetrics().associateBy { it.name }
            assertFalse(beforePositive.containsKey("khrona.recovery.stale"))
            assertFalse(beforePositive.containsKey("khrona.retention.deleted"))

            observer.onExecutionScheduled("otel-sync", ScheduleReason.RETRY)
            observer.onExecutionStarted(ctx, Duration.ofMillis(250))
            observer.onExecutionStarted(ctx, Duration.ofMillis(-25))
            observer.onHeartbeatSucceeded(ctx)
            observer.onHeartbeatFailed(ctx, HeartbeatFailureReason.LEASE_BUDGET_EXCEEDED)
            observer.onRetryScheduled(ctx, nextAttempt = 3, delay = Duration.ofSeconds(2), errorType = "TimeoutCancellationException")
            observer.onExecutionCompleted(ctx, ExecutionOutcome.SUCCESS, Duration.ofMillis(500), errorType = null)
            observer.onExecutionCompleted(ctx, ExecutionOutcome.DEAD_LETTERED, Duration.ofMillis(-5), errorType = "IllegalStateException")
            observer.onStaleExecutionsRecovered(4)
            observer.onStaleExecutionsRecovered(-3)
            observer.onLockContention("otel-sync", "dynamic-lock-secret", ConcurrencyPolicy.FORBID)
            observer.onDeadLetterCountSampled(9L)
            observer.onRetentionCleanup(6)
            observer.onRetentionCleanup(-1)
            observer.onShutdownCompleted(Duration.ofMillis(100), clean = true, remainingActiveCount = 0)
            observer.onShutdownCompleted(Duration.ofMillis(-10), clean = false, remainingActiveCount = 1)
            observer.onStoreOperation("heartbeat", Duration.ofMillis(12), success = true, errorType = null)
            observer.onStoreOperation("heartbeat", Duration.ofMillis(-2), success = false, errorType = "RedisException")

            val metricsByName = metricReader.collectAllMetrics().associateBy { it.name }

            val lagPoints = metricsByName.getValue("khrona.execution.lag").histogramData.points
            assertEquals(2L, lagPoints.sumOf { it.count })
            assertEquals(0.25, lagPoints.sumOf { it.sum }, 0.001)

            val durationPoints = metricsByName.getValue("khrona.execution.duration").histogramData.points
            assertEquals(2L, durationPoints.sumOf { it.count })
            assertEquals(0.5, durationPoints.sumOf { it.sum }, 0.001)

            assertEquals(
                2L,
                metricsByName.getValue("khrona.executions.started").longSumData.points.sumOf { it.value }
            )
            assertEquals(
                1L,
                metricsByName.getValue("khrona.executions.scheduled").longSumData.points.sumOf { it.value }
            )
            val completedPoints = metricsByName.getValue("khrona.executions.completed").longSumData.points
            assertEquals(2L, completedPoints.sumOf { it.value })
            assertEquals(
                1L,
                completedPoints.single {
                    it.attributes.get(AttributeKey.stringKey("outcome")) == "success" &&
                        it.attributes.get(AttributeKey.stringKey("error_type")) == "none"
                }.value
            )
            assertEquals(
                1L,
                completedPoints.single {
                    it.attributes.get(AttributeKey.stringKey("outcome")) == "dead_lettered" &&
                        it.attributes.get(AttributeKey.stringKey("error_type")) == "IllegalStateException"
                }.value
            )
            assertEquals(
                1L,
                metricsByName.getValue("khrona.executions.retried").longSumData.points.sumOf { it.value }
            )
            val hbPoints = metricsByName.getValue("khrona.heartbeat.total").longSumData.points
            assertEquals(2L, hbPoints.sumOf { it.value })
            assertEquals(
                1L,
                hbPoints.single {
                    it.attributes.get(AttributeKey.stringKey("status")) == "succeeded" &&
                        it.attributes.get(AttributeKey.stringKey("reason")) == "none"
                }.value
            )
            assertEquals(
                1L,
                hbPoints.single {
                    it.attributes.get(AttributeKey.stringKey("status")) == "failed" &&
                        it.attributes.get(AttributeKey.stringKey("reason")) == "lease_budget_exceeded"
                }.value
            )
            assertEquals(
                4L,
                metricsByName.getValue("khrona.recovery.stale").longSumData.points.sumOf { it.value }
            )
            assertEquals(
                1L,
                metricsByName.getValue("khrona.lock.contention").longSumData.points.sumOf { it.value }
            )
            assertEquals(
                9L,
                metricsByName.getValue("khrona.dead_letter.size").longGaugeData.points.single().value
            )
            observer.onDeadLetterCountSampled(-3L)
            assertEquals(
                0L,
                metricReader.collectAllMetrics().associateBy { it.name }
                    .getValue("khrona.dead_letter.size").longGaugeData.points.single().value
            )
            assertEquals(
                6L,
                metricsByName.getValue("khrona.retention.deleted").longSumData.points.sumOf { it.value }
            )
            val shutdownPoints = metricsByName.getValue("khrona.shutdown.duration").histogramData.points
            assertEquals(2L, shutdownPoints.sumOf { it.count })
            assertEquals(0.1, shutdownPoints.sumOf { it.sum }, 0.001)
            val storeOpPoints = metricsByName.getValue("khrona.store.operation.duration").histogramData.points
            assertEquals(2L, storeOpPoints.sumOf { it.count })
            assertEquals(
                1L,
                storeOpPoints.single {
                    it.attributes.get(AttributeKey.stringKey("status")) == "success" &&
                        it.attributes.get(AttributeKey.stringKey("error_type")) == "none"
                }.count
            )
            assertEquals(
                1L,
                storeOpPoints.single {
                    it.attributes.get(AttributeKey.stringKey("status")) == "error" &&
                        it.attributes.get(AttributeKey.stringKey("error_type")) == "RedisException"
                }.count
            )

            // Verify zero unbounded attribute keys/values in all exported metrics
            val forbiddenKeys = setOf("execution_id", "executionId", "khrona.execution.id", "correlation_id", "correlationId", "khrona.correlation_id", "worker_id", "workerId", "lock_key", "lockKey")
            metricsByName.values.forEach { metric ->
                val allPoints = metric.longSumData.points +
                    metric.histogramData.points +
                    metric.longGaugeData.points
                allPoints.forEach { point ->
                    point.attributes.forEach { key, value ->
                        assertFalse(key.key in forbiddenKeys, "Unbounded metric attribute leaked: ${key.key}")
                        val rendered = value.toString()
                        assertFalse(rendered.contains(ctx.executionId.toString()))
                        assertFalse(rendered.contains(ctx.correlationId))
                        assertFalse(rendered.contains("dynamic-lock-secret"))
                        assertFalse(rendered.contains("secret-payload"))
                    }
                }
            }
        } finally {
            observer.close()
            sdk.close()
        }
    }
}
