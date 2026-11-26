package io.khrona.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
class ObserverIsolationRegressionTest {
    @Test
    fun `a timeout local to dead letter sampling leaves the polling loop healthy`() = runTest {
        var sampleAttempts = 0
        val backend = object : MockJobStore() {
            override suspend fun countExecutions(query: JobExecutionQuery): Long {
                sampleAttempts++
                return withTimeout(5) {
                    delay(10)
                    0L
                }
            }
        }
        val config = KhronaConfig().apply {
            store = backend
            observer = object : KhronaObserver {}
        }
        val scheduler = Scheduler(config, backgroundScope)

        scheduler.start()
        advanceTimeBy(6)

        assertEquals(1, sampleAttempts)
        assertEquals(HealthStatus.UP, scheduler.health().status)
    }

    @Test
    fun `observer failure while the handler is suspended does not rerun it`() = runTest {
        val releaseHandler = CompletableDeferred<Unit>()
        var handlerCalls = 0
        val observer = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                // Start the handler up to its first suspension before the observer fails.
                backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { block() }
                error("Observer failed while the handler was suspended")
            }
        }
        val context = ExecutionContext(
            executionId = UUID.randomUUID(),
            jobId = "test-job",
            attempt = 0,
            scheduledAt = Instant.EPOCH,
            payload = null,
            correlationId = "test-correlation",
            isStopping = { false }
        )

        val execution = async {
            invokeAroundSafely(observer, context) {
                handlerCalls++
                releaseHandler.await()
                "handler result"
            }
        }
        runCurrent()
        releaseHandler.complete(Unit)

        assertEquals("handler result", execution.await())
        assertEquals(1, handlerCalls)
    }

    @Test
    fun `failed remote lookup does not attribute supersession to the replacing job`() = runTest {
        val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
        val base = MockJobStore(clock)
        val remote = JobExecution(
            jobId = "older-job",
            lockKey = "shared",
            status = ExecutionStatus.RUNNING,
            workerId = "other-node",
            scheduledAt = Instant.EPOCH,
            expiresAt = Instant.EPOCH.plusSeconds(60)
        )
        val replacement = JobExecution(jobId = "newer-job", lockKey = remote.lockKey, scheduledAt = Instant.EPOCH)
        val backend = object : JobStore by base {
            override suspend fun getExecution(id: UUID): JobExecution? {
                if (id == remote.id) error("Superseded execution lookup failed")
                return base.getExecution(id)
            }
        }
        val supersededJobs = mutableListOf<String>()
        val config = KhronaConfig().apply {
            store = backend
            observer = object : KhronaObserver {
                override fun onExecutionCompleted(
                    context: ExecutionContext,
                    outcome: ExecutionOutcome,
                    duration: Duration,
                    errorType: String?
                ) {
                    if (outcome == ExecutionOutcome.SUPERSEDED) supersededJobs.add(context.jobId)
                }
            }
            job(replacement.jobId) {
                at(Instant.EPOCH.plusSeconds(3600))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = replacement.lockKey
                execute {}
            }
        }
        val scheduler = Scheduler(config, backgroundScope, clock)
        base.saveJob(config.jobs.single())
        base.saveExecution(remote)
        base.saveExecution(replacement)

        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(ExecutionStatus.SUPERSEDED, base.getExecution(remote.id)?.status)
        assertFalse(replacement.jobId in supersededJobs, "Reported superseded jobs: $supersededJobs")
    }
}
