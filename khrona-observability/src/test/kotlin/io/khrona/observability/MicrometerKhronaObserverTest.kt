package io.khrona.observability

import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.ExecutionContext
import io.khrona.core.ExecutionOutcome
import io.khrona.core.HeartbeatFailureReason
import io.khrona.core.KhronaConfig
import io.khrona.core.ScheduleReason
import io.khrona.core.Scheduler
import io.khrona.core.observedBy
import io.khrona.store.memory.MemoryJobStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class MicrometerKhronaObserverTest {

    private fun sampleContext(
        executionId: UUID = UUID.fromString("11111111-2222-3333-4444-555555555555"),
        jobId: String = "billing-sync",
        attempt: Int = 1,
        correlationId: String = "corr-secret-999"
    ): ExecutionContext = ExecutionContext(
        executionId = executionId,
        jobId = jobId,
        attempt = attempt,
        scheduledAt = Instant.parse("2030-01-01T00:00:00Z"),
        payload = mapOf("secret" to "top-secret-payload"),
        correlationId = correlationId,
        isStopping = { false }
    )

    @Test
    fun `rejects blank metric prefix`() {
        val registry = SimpleMeterRegistry()
        assertThrows(IllegalArgumentException::class.java) {
            MicrometerKhronaObserver(registry, metricPrefix = "   ")
        }
    }

    @Test
    fun `records full catalog of Micrometer meters with bounded tags and consistent keys`() {
        val registry = SimpleMeterRegistry()
        val observer = MicrometerKhronaObserver(registry, metricPrefix = "custom.khrona")
        val ctx = sampleContext()

        observer.onExecutionScheduled("billing-sync", ScheduleReason.INITIAL_SEED)
        observer.onExecutionScheduled("billing-sync", ScheduleReason.MANUAL_TRIGGER)
        observer.onExecutionStarted(ctx, Duration.ofMillis(125))
        observer.onExecutionStarted(ctx, Duration.ofMillis(-50)) // clamped to zero
        observer.onHeartbeatSucceeded(ctx)
        observer.onHeartbeatFailed(ctx, HeartbeatFailureReason.STORE_ERROR)
        observer.onRetryScheduled(ctx, nextAttempt = 2, delay = Duration.ofSeconds(1), errorType = "IOException")
        observer.onExecutionCompleted(ctx, ExecutionOutcome.SUCCESS, Duration.ofMillis(200), errorType = null)
        observer.onExecutionCompleted(ctx, ExecutionOutcome.FAILED_RETRY, Duration.ofMillis(-10), errorType = "IOException")
        observer.onStaleExecutionsRecovered(0)
        assertNull(registry.find("custom.khrona.recovery.stale").counter())
        observer.onStaleExecutionsRecovered(-2)
        assertNull(registry.find("custom.khrona.recovery.stale").counter())
        observer.onStaleExecutionsRecovered(3)
        observer.onLockContention("billing-sync", "dynamic-lock-key-abc", ConcurrencyPolicy.FORBID)
        observer.onDeadLetterCountSampled(7L)
        observer.onRetentionCleanup(0)
        assertNull(registry.find("custom.khrona.retention.deleted").counter())
        observer.onRetentionCleanup(-1)
        assertNull(registry.find("custom.khrona.retention.deleted").counter())
        observer.onRetentionCleanup(4)
        observer.onShutdownCompleted(Duration.ofMillis(75), clean = true, remainingActiveCount = 0)
        observer.onShutdownCompleted(Duration.ofMillis(-5), clean = false, remainingActiveCount = 2)
        observer.onStoreOperation("claimExecution", Duration.ofMillis(5), success = true, errorType = null)
        observer.onStoreOperation("completeExecution", Duration.ofMillis(-3), success = false, errorType = "SQLException")

        assertEquals(
            1.0,
            registry.get("custom.khrona.executions.scheduled")
                .tag("job_id", "billing-sync")
                .tag("reason", "initial_seed")
                .counter().count()
        )
        assertEquals(
            1.0,
            registry.get("custom.khrona.executions.scheduled")
                .tag("job_id", "billing-sync")
                .tag("reason", "manual_trigger")
                .counter().count()
        )

        val lagTimer = registry.get("custom.khrona.execution.lag").tag("job_id", "billing-sync").timer()
        assertEquals(2L, lagTimer.count())
        assertEquals(125.0, lagTimer.totalTime(TimeUnit.MILLISECONDS), 0.1)

        assertEquals(
            2.0,
            registry.get("custom.khrona.executions.started").tag("job_id", "billing-sync").counter().count()
        )

        val successCompleted = registry.get("custom.khrona.executions.completed")
            .tag("job_id", "billing-sync")
            .tag("outcome", "success")
            .tag("error_type", "none")
            .counter()
        val failedCompleted = registry.get("custom.khrona.executions.completed")
            .tag("job_id", "billing-sync")
            .tag("outcome", "failed_retry")
            .tag("error_type", "IOException")
            .counter()
        assertEquals(1.0, successCompleted.count())
        assertEquals(1.0, failedCompleted.count())
        assertEquals(
            successCompleted.id.tags.map { it.key }.toSet(),
            failedCompleted.id.tags.map { it.key }.toSet()
        )

        val successDuration = registry.get("custom.khrona.execution.duration")
            .tag("job_id", "billing-sync")
            .tag("outcome", "success")
            .timer()
        assertEquals(1L, successDuration.count())
        assertEquals(200.0, successDuration.totalTime(TimeUnit.MILLISECONDS), 0.1)
        val retryDuration = registry.get("custom.khrona.execution.duration")
            .tag("job_id", "billing-sync")
            .tag("outcome", "failed_retry")
            .timer()
        assertEquals(1L, retryDuration.count())
        assertEquals(0.0, retryDuration.totalTime(TimeUnit.MILLISECONDS), 0.1)

        assertEquals(
            1.0,
            registry.get("custom.khrona.executions.retried")
                .tag("job_id", "billing-sync")
                .tag("error_type", "IOException")
                .counter().count()
        )

        val hbOk = registry.get("custom.khrona.heartbeat.total")
            .tag("job_id", "billing-sync")
            .tag("status", "succeeded")
            .tag("reason", "none")
            .counter()
        val hbErr = registry.get("custom.khrona.heartbeat.total")
            .tag("job_id", "billing-sync")
            .tag("status", "failed")
            .tag("reason", "store_error")
            .counter()
        assertEquals(1.0, hbOk.count())
        assertEquals(1.0, hbErr.count())
        assertEquals(hbOk.id.tags.map { it.key }.toSet(), hbErr.id.tags.map { it.key }.toSet())

        assertEquals(3.0, registry.get("custom.khrona.recovery.stale").counter().count())
        assertEquals(
            1.0,
            registry.get("custom.khrona.lock.contention")
                .tag("job_id", "billing-sync")
                .tag("policy", "forbid")
                .counter().count()
        )
        assertEquals(7.0, registry.get("custom.khrona.dead_letter.size").gauge().value(), 0.0)
        observer.onDeadLetterCountSampled(-4L)
        assertEquals(0.0, registry.get("custom.khrona.dead_letter.size").gauge().value(), 0.0)

        assertEquals(4.0, registry.get("custom.khrona.retention.deleted").counter().count())
        assertEquals(
            75.0,
            registry.get("custom.khrona.shutdown.duration").tag("clean", "true").timer()
                .totalTime(TimeUnit.MILLISECONDS),
            0.1
        )
        assertEquals(
            0.0,
            registry.get("custom.khrona.shutdown.duration").tag("clean", "false").timer()
                .totalTime(TimeUnit.MILLISECONDS),
            0.1
        )

        val storeOk = registry.get("custom.khrona.store.operation.duration")
            .tag("operation", "claimExecution")
            .tag("status", "success")
            .tag("error_type", "none")
            .timer()
        val storeErr = registry.get("custom.khrona.store.operation.duration")
            .tag("operation", "completeExecution")
            .tag("status", "error")
            .tag("error_type", "SQLException")
            .timer()
        assertEquals(1L, storeOk.count())
        assertEquals(1L, storeErr.count())
        assertEquals(storeOk.id.tags.map { it.key }.toSet(), storeErr.id.tags.map { it.key }.toSet())

        // Verify zero unbounded identifier leakage in any registered meter
        val forbiddenTagKeys = setOf("execution_id", "executionId", "correlation_id", "correlationId", "worker_id", "workerId", "lock_key", "lockKey")
        registry.meters.forEach { meter ->
            meter.id.tags.forEach { tag ->
                assertFalse(tag.key in forbiddenTagKeys, "Unbounded tag key leaked: ${tag.key}")
                assertFalse(tag.value.contains(ctx.executionId.toString()))
                assertFalse(tag.value.contains(ctx.correlationId))
                assertFalse(tag.value.contains("dynamic-lock-key-abc"))
                assertFalse(tag.value.contains("top-secret-payload"))
            }
        }
    }

    @Test
    fun `integrates with Scheduler and ObservableJobStore to record lifecycle and store metrics`() = runBlocking {
        val registry = SimpleMeterRegistry()
        val observer = MicrometerKhronaObserver(registry)
        val observedStore = MemoryJobStore().observedBy(observer)
        val okDone = CompletableDeferred<Unit>()
        val retryAttempts = AtomicInteger(0)
        val retryDone = CompletableDeferred<Unit>()
        val dlqDone = CompletableDeferred<Unit>()

        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = observedStore
                this.observer = observer
                pollingInterval = Duration.ofMillis(30)
                deadLetterSamplingInterval = Duration.ofMillis(40)
                shutdownTimeout = Duration.ofSeconds(2)

                job("micrometer-ok") {
                    once()
                    execute { okDone.complete(Unit) }
                }

                job("micrometer-retry") {
                    once()
                    retry {
                        maxAttempts = 2
                        initialDelay = Duration.ofMillis(40)
                    }
                    execute {
                        if (retryAttempts.incrementAndGet() == 1) {
                            throw IllegalStateException("sensitive failure message secret-123")
                        }
                        retryDone.complete(Unit)
                    }
                }

                job("micrometer-dlq") {
                    once()
                    retry { maxAttempts = 1 }
                    execute {
                        dlqDone.complete(Unit)
                        throw IllegalArgumentException("terminal failure secret-456")
                    }
                }
            },
            scope
        )

        try {
            scheduler.start()
            withTimeout(5_000) {
                okDone.await()
                retryDone.await()
                dlqDone.await()
                while ((registry.find("khrona.dead_letter.size").gauge()?.value() ?: 0.0) < 1.0) {
                    delay(20)
                }
                while (
                    (registry.find("khrona.executions.completed")
                        .tag("job_id", "micrometer-ok")
                        .tag("outcome", "success")
                        .counter()?.count() ?: 0.0) < 1.0
                ) {
                    delay(20)
                }
            }
            scheduler.stop()

            assertEquals(1.0, registry.get("khrona.executions.completed").tag("job_id", "micrometer-ok").tag("outcome", "success").counter().count())
            assertEquals(1.0, registry.get("khrona.executions.retried").tag("job_id", "micrometer-retry").tag("error_type", "IllegalStateException").counter().count())
            assertEquals(1.0, registry.get("khrona.executions.completed").tag("job_id", "micrometer-dlq").tag("outcome", "dead_lettered").tag("error_type", "IllegalArgumentException").counter().count())
            assertTrue(registry.get("khrona.store.operation.duration").tag("operation", "listEligibleExecutions").timer().count() >= 1L)
            assertEquals(1L, registry.get("khrona.shutdown.duration").tag("clean", "true").timer().count())

            registry.meters.forEach { meter ->
                meter.id.tags.forEach { tag ->
                    assertFalse(tag.value.contains("secret-123"))
                    assertFalse(tag.value.contains("secret-456"))
                }
            }
        } finally {
            scope.cancel()
        }
    }
}
