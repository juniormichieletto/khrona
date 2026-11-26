@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ObservabilityTest {

    private class TraceTag(val value: String) : AbstractCoroutineContextElement(Key) {
        companion object Key : CoroutineContext.Key<TraceTag>
    }

    private fun sampleContext(
        id: UUID = UUID.randomUUID(),
        jobId: String = "obs-job",
        isStopping: () -> Boolean = { false }
    ): ExecutionContext = ExecutionContext(
        executionId = id,
        jobId = jobId,
        attempt = 0,
        scheduledAt = Instant.parse("2030-01-01T00:00:00Z"),
        payload = "payload",
        correlationId = "corr-$id",
        isStopping = isStopping
    )

    @Test
    fun `ErrorDiagnosticFormatter preserves error type truncates at maxLength and supports redaction`() {
        assertEquals(
            "IllegalStateException: disk full",
            ErrorDiagnosticFormatter.DEFAULT.format(IllegalStateException("  disk full  "))
        )
        assertEquals(
            "IllegalStateException",
            ErrorDiagnosticFormatter.DEFAULT.format(IllegalStateException())
        )
        assertEquals(
            "IllegalArgumentException",
            ErrorDiagnosticFormatter.DEFAULT.format(IllegalArgumentException("   "))
        )

        val anonymous = object : RuntimeException("boom") {}
        assertTrue(ErrorDiagnosticFormatter.DEFAULT.format(anonymous).endsWith(": boom"))
        assertEquals(
            anonymous.javaClass.name,
            ErrorDiagnosticFormatter.typeName(anonymous)
        )

        assertThrows(IllegalArgumentException::class.java) {
            ErrorDiagnosticFormatter.bounded(3)
        }

        val exactFormatter = ErrorDiagnosticFormatter.bounded(10)
        // "Error: abc" has length 10 -> not truncated
        assertEquals("Error: abc", exactFormatter.format(Error("abc")))
        // "Error: abcd" has length 11 -> truncated to 7 chars + "..." = 10
        assertEquals("Error: ...", exactFormatter.format(Error("abcd")))

        val defaultMaxBoundary = "x".repeat(ErrorDiagnosticFormatter.DEFAULT_MAX_LENGTH)
        val formattedDefaultBoundary = ErrorDiagnosticFormatter.DEFAULT.format(IllegalStateException(defaultMaxBoundary))
        assertEquals(ErrorDiagnosticFormatter.DEFAULT_MAX_LENGTH, formattedDefaultBoundary.length)
        assertTrue(formattedDefaultBoundary.endsWith("..."))

        val redacting = ErrorDiagnosticFormatter { err ->
            ErrorDiagnosticFormatter.DEFAULT.format(err).replace(Regex("token=\\w+"), "token=***")
        }
        assertEquals(
            "IllegalStateException: auth failed token=***",
            redacting.format(IllegalStateException("auth failed token=secret123"))
        )
    }

    @Test
    fun `KhronaObserver NO_OP handles every callback cleanly`() = runTest {
        val ctx = sampleContext()
        val result = KhronaObserver.NO_OP.aroundExecution(ctx) { "ok" }
        assertEquals("ok", result)
        KhronaObserver.NO_OP.onExecutionScheduled("job", ScheduleReason.INITIAL_SEED)
        KhronaObserver.NO_OP.onExecutionStarted(ctx, Duration.ZERO)
        KhronaObserver.NO_OP.onExecutionCompleted(ctx, ExecutionOutcome.SUCCESS, Duration.ofMillis(1))
        KhronaObserver.NO_OP.onRetryScheduled(ctx, 1, Duration.ofSeconds(1), "RuntimeException")
        KhronaObserver.NO_OP.onHeartbeatSucceeded(ctx)
        KhronaObserver.NO_OP.onHeartbeatFailed(ctx, HeartbeatFailureReason.STORE_ERROR)
        KhronaObserver.NO_OP.onStaleExecutionsRecovered(2)
        KhronaObserver.NO_OP.onLockContention("job", "lock", ConcurrencyPolicy.FORBID)
        KhronaObserver.NO_OP.onDeadLetterCountSampled(3L)
        KhronaObserver.NO_OP.onRetentionCleanup(4)
        KhronaObserver.NO_OP.onShutdownCompleted(Duration.ofMillis(10), clean = true, remainingActiveCount = 0)
        KhronaObserver.NO_OP.onStoreOperation("getJob", Duration.ofMillis(2), success = true)
    }

    @Test
    fun `CompositeKhronaObserver dispatches all signals isolates failures and chains aroundExecution`() = runTest {
        val ctx = sampleContext()
        val recorded = mutableListOf<String>()
        val throwingObserver = object : KhronaObserver {
            override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) = error("boom")
            override fun onExecutionStarted(context: ExecutionContext, lag: Duration) = error("boom")
            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) = error("boom")

            override fun onRetryScheduled(context: ExecutionContext, nextAttempt: Int, delay: Duration, errorType: String) =
                error("boom")

            override fun onHeartbeatSucceeded(context: ExecutionContext) = error("boom")
            override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) = error("boom")
            override fun onStaleExecutionsRecovered(count: Int) = error("boom")
            override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) = error("boom")
            override fun onDeadLetterCountSampled(count: Long) = error("boom")
            override fun onRetentionCleanup(deletedCount: Int) = error("boom")
            override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) = error("boom")
            override fun onStoreOperation(operation: String, duration: Duration, success: Boolean, errorType: String?) =
                error("boom")
        }
        val recordingObserver = object : KhronaObserver {
            override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {
                recorded.add("scheduled:$jobId:$reason")
            }

            override fun onExecutionStarted(context: ExecutionContext, lag: Duration) {
                recorded.add("started:${context.jobId}:$lag")
            }

            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                recorded.add("completed:$outcome:$errorType")
            }

            override fun onRetryScheduled(context: ExecutionContext, nextAttempt: Int, delay: Duration, errorType: String) {
                recorded.add("retry:$nextAttempt:$errorType")
            }

            override fun onHeartbeatSucceeded(context: ExecutionContext) {
                recorded.add("hb_ok")
            }

            override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {
                recorded.add("hb_fail:$reason")
            }

            override fun onStaleExecutionsRecovered(count: Int) {
                recorded.add("stale:$count")
            }

            override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {
                recorded.add("lock:$lockKey:$policy")
            }

            override fun onDeadLetterCountSampled(count: Long) {
                recorded.add("dlq:$count")
            }

            override fun onRetentionCleanup(deletedCount: Int) {
                recorded.add("cleanup:$deletedCount")
            }

            override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {
                recorded.add("shutdown:$clean:$remainingActiveCount")
            }

            override fun onStoreOperation(operation: String, duration: Duration, success: Boolean, errorType: String?) {
                recorded.add("store:$operation:$success:$errorType")
            }
        }

        val composite = CompositeKhronaObserver(listOf(throwingObserver, recordingObserver))
        composite.onExecutionScheduled("j1", ScheduleReason.MANUAL_TRIGGER)
        composite.onExecutionStarted(ctx, Duration.ofMillis(5))
        composite.onExecutionCompleted(ctx, ExecutionOutcome.FAILED_RETRY, Duration.ofMillis(12), "IllegalStateException")
        composite.onRetryScheduled(ctx, 1, Duration.ofSeconds(2), "IllegalStateException")
        composite.onHeartbeatSucceeded(ctx)
        composite.onHeartbeatFailed(ctx, HeartbeatFailureReason.LEASE_BUDGET_EXCEEDED)
        composite.onStaleExecutionsRecovered(3)
        composite.onLockContention("j1", "lk", ConcurrencyPolicy.FORBID)
        composite.onDeadLetterCountSampled(7L)
        composite.onRetentionCleanup(9)
        composite.onShutdownCompleted(Duration.ofMillis(20), clean = false, remainingActiveCount = 2)
        composite.onStoreOperation("saveJob", Duration.ofMillis(3), success = false, errorType = "SQLException")

        assertEquals(
            listOf(
                "scheduled:j1:MANUAL_TRIGGER",
                "started:obs-job:PT0.005S",
                "completed:FAILED_RETRY:IllegalStateException",
                "retry:1:IllegalStateException",
                "hb_ok",
                "hb_fail:LEASE_BUDGET_EXCEEDED",
                "stale:3",
                "lock:lk:FORBID",
                "dlq:7",
                "cleanup:9",
                "shutdown:false:2",
                "store:saveJob:false:SQLException"
            ),
            recorded
        )

        // aroundExecution chaining + context propagation
        val order = mutableListOf<String>()
        val first = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                order.add("enter-1")
                return withContext(TraceTag("span-1")) {
                    try {
                        block()
                    } finally {
                        order.add("exit-1")
                    }
                }
            }
        }
        val second = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                order.add("enter-2:${currentCoroutineContext()[TraceTag]?.value}")
                return withContext(CoroutineName("exec-2")) {
                    try {
                        block()
                    } finally {
                        order.add("exit-2")
                    }
                }
            }
        }
        val chained = CompositeKhronaObserver(listOf(first, second))
        val observedNames = chained.aroundExecution(ctx) {
            "${currentCoroutineContext()[TraceTag]?.value}:${currentCoroutineContext()[CoroutineName]?.name}"
        }
        assertEquals("span-1:exec-2", observedNames)
        assertEquals(listOf("enter-1", "enter-2:span-1", "exit-2", "exit-1"), order)

        // aroundExecution isolates observer failures before/after block without hiding handler result or handler error
        val throwsBefore = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                throw IllegalStateException("before")
            }
        }
        val throwsAfter = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                block()
                throw IllegalStateException("after")
            }
        }
        val resilientComposite = CompositeKhronaObserver(listOf(throwsBefore, throwsAfter))
        assertEquals("handler-ok", resilientComposite.aroundExecution(ctx) { "handler-ok" })

        val handlerBoom = IllegalArgumentException("handler-boom")
        val thrown = assertThrows(IllegalArgumentException::class.java) {
            kotlinx.coroutines.runBlocking {
                resilientComposite.aroundExecution(ctx) { throw handlerBoom }
            }
        }
        assertSame(handlerBoom, thrown)

        // CancellationException thrown by handler is propagated unchanged
        assertThrows(CancellationException::class.java) {
            kotlinx.coroutines.runBlocking {
                resilientComposite.aroundExecution(ctx) { throw CancellationException("cancelled") }
            }
        }
    }

    @Test
    fun `KhronaConfig validates observability settings and composes observers`() {
        val obs1 = object : KhronaObserver {}
        val obs2 = object : KhronaObserver {}
        val obs3 = object : KhronaObserver {}

        val config = KhronaConfig().apply {
            store = MockJobStore()
            addObserver(KhronaObserver.NO_OP)
            assertSame(KhronaObserver.NO_OP, observer)

            addObserver(obs1)
            assertSame(obs1, observer)

            addObserver(obs2)
            assertTrue(observer is CompositeKhronaObserver)
            assertEquals(listOf(obs1, obs2), (observer as CompositeKhronaObserver).observers)

            addObserver(obs3)
            assertEquals(listOf(obs1, obs2, obs3), (observer as CompositeKhronaObserver).observers)

            healthPollFailureThreshold = 1
            deadLetterSamplingInterval(15.seconds)
        }
        config.validate()
        assertEquals(Duration.ofSeconds(15), config.deadLetterSamplingInterval)

        assertThrows(IllegalArgumentException::class.java) {
            KhronaConfig().apply {
                store = MockJobStore()
                healthPollFailureThreshold = 0
            }.validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            KhronaConfig().apply {
                store = MockJobStore()
                deadLetterSamplingInterval = Duration.ZERO
            }.validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            KhronaConfig().apply {
                store = MockJobStore()
                deadLetterSamplingInterval = Duration.ofSeconds(-1)
            }.validate()
        }
    }

    @Test
    fun `ObservableJobStore delegates all operations and records store latency and failures`() = runTest {
        val delegate = MockJobStore()
        val operations = mutableListOf<Triple<String, Boolean, String?>>()
        val recorder = object : KhronaObserver {
            override fun onStoreOperation(operation: String, duration: Duration, success: Boolean, errorType: String?) {
                assertFalse(duration.isNegative)
                operations.add(Triple(operation, success, errorType))
            }
        }
        val observed = delegate.observedBy(recorder)
        val job = JobDefinition(id = "obs-store-job", trigger = IntervalTrigger(Duration.ofMinutes(1)))
        val exec = JobExecution(jobId = job.id, scheduledAt = Instant.EPOCH, lockKey = "obs-lock")

        observed.saveJob(job)
        observed.getJob(job.id)
        observed.listJobs()
        observed.seedInitialExecution(exec)
        observed.getExecution(exec.id)
        observed.listExecutions(JobExecutionQuery(jobId = job.id))
        observed.countExecutions(JobExecutionQuery(jobId = job.id))
        observed.listEligibleExecutions(Instant.now(), 10)
        observed.listEligibleExecutions(Instant.now(), 10, setOf(job.id))
        observed.claimExecution(exec.id, "w1", Duration.ofMinutes(1))
        observed.heartbeat(exec.id, "w1", Duration.ofMinutes(1))
        observed.isLockHeld("obs-lock")
        observed.updateExecutionStatus(exec.id, ExecutionStatus.RUNNING, workerId = "w1")
        observed.completeExecution(exec.id, ExecutionStatus.SUCCESS, workerId = "w1")
        observed.supersedeExecutionsByLockKey("obs-lock")
        observed.resetExpiredExecutions(Instant.now())
        observed.cleanupCompletedExecutions(Instant.now().plusSeconds(60))
        assertEquals(HealthStatus.UP, observed.checkHealth().status)

        assertTrue(operations.all { it.second && it.third == null })
        assertEquals(18, operations.size)

        // Error and CancellationException handling
        val failingStore = object : JobStore by delegate {
            override suspend fun getJob(jobId: String): JobDefinition? = throw IllegalStateException("db down")
            override suspend fun listJobs(): List<JobDefinition> = throw CancellationException("cancelled")
        }.observedBy(recorder)

        assertThrows(IllegalStateException::class.java) {
            kotlinx.coroutines.runBlocking { failingStore.getJob("x") }
        }
        assertEquals(Triple("getJob", false, "IllegalStateException"), operations.last())

        val beforeCancelCount = operations.size
        assertThrows(CancellationException::class.java) {
            kotlinx.coroutines.runBlocking { failingStore.listJobs() }
        }
        assertEquals(beforeCancelCount, operations.size)
    }

    private class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    @Test
    fun `Scheduler emits lifecycle scheduling lag outcomes retries dead-letter diagnostics and aroundExecution propagation`() = runTest {
        val clock = TestClock(testScheduler)
        val baseDelegate = MockJobStore(clock)
        val forceZeroLagNextPoll = AtomicBoolean(false)
        val suppressInitialSeedForJob = AtomicReference<String?>(null)
        val store = object : JobStore by baseDelegate {
            override suspend fun seedInitialExecution(execution: JobExecution): Boolean {
                if (suppressInitialSeedForJob.get() == execution.jobId) return false
                return baseDelegate.seedInitialExecution(execution)
            }

            override suspend fun listEligibleExecutions(
                now: Instant,
                limit: Int,
                jobIds: Set<String>?
            ): List<JobExecution> {
                val eligible = baseDelegate.listEligibleExecutions(now, limit, jobIds)
                if (!forceZeroLagNextPoll.compareAndSet(true, false)) return eligible
                return eligible.map {
                    if (it.jobId == "skew-job") it.copy(scheduledAt = now.plusSeconds(10)) else it
                }
            }
        }

        val scheduledEvents = CopyOnWriteArrayList<Pair<String, ScheduleReason>>()
        val startedEvents = CopyOnWriteArrayList<Pair<ExecutionContext, Duration>>()
        val completedEvents = CopyOnWriteArrayList<Triple<ExecutionContext, ExecutionOutcome, String?>>()
        val retryEvents = CopyOnWriteArrayList<Triple<String, Int, String>>()
        val observedContextTag = AtomicReference<String?>(null)

        val observer = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                return withContext(TraceTag("trace-${context.jobId}-${context.attempt}")) {
                    block()
                }
            }

            override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) {
                scheduledEvents.add(jobId to reason)
            }

            override fun onExecutionStarted(context: ExecutionContext, lag: Duration) {
                startedEvents.add(context to lag)
            }

            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                assertFalse(duration.isNegative)
                completedEvents.add(Triple(context, outcome, errorType))
            }

            override fun onRetryScheduled(
                context: ExecutionContext,
                nextAttempt: Int,
                delay: Duration,
                errorType: String
            ) {
                retryEvents.add(Triple(context.jobId, nextAttempt, errorType))
            }
        }

        val replaceStarted = CompletableDeferred<UUID>()
        val cancelStarted = CompletableDeferred<UUID>()
        val intervalRuns = AtomicInteger(0)
        val flakyAttempts = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            this.observer = observer
            this.errorFormatter = ErrorDiagnosticFormatter { err ->
                val formatted = ErrorDiagnosticFormatter.DEFAULT.format(err)
                formatted.replace("secret-token-123", "[REDACTED]")
            }
            pollingInterval = Duration.ofMillis(100)
            misfireThreshold = Duration.ofSeconds(5)
            executionLeaseDuration = Duration.ofSeconds(10)
            heartbeatInterval = Duration.ofSeconds(2)
            shutdownTimeout = Duration.ofSeconds(2)

            job("interval-ok") {
                every(Duration.ofSeconds(1))
                execute {
                    if (intervalRuns.incrementAndGet() == 1) {
                        delay(1)
                        observedContextTag.set(currentCoroutineContext()[TraceTag]?.value)
                    }
                }
            }

            job("null-msg-dlq") {
                once()
                retry {
                    maxAttempts = 1
                }
                execute {
                    throw IllegalStateException()
                }
            }

            job("flaky-job") {
                at(Instant.now(clock).plusSeconds(3600))
                retry {
                    maxAttempts = 2
                    initialDelay = Duration.ofMillis(100)
                    jitter = 0.0
                }
                execute {
                    val current = flakyAttempts.incrementAndGet()
                    if (current == 1) {
                        throw IllegalArgumentException("failed using secret-token-123")
                    }
                }
            }

            job("misfire-ignore") {
                at(Instant.now(clock).plusSeconds(3600))
                misfirePolicy = MisfirePolicy.IGNORE
                execute {}
            }

            job("skew-job") {
                at(Instant.now(clock).plusSeconds(3600))
                execute {}
            }

            job("replace-job") {
                at(Instant.now(clock).plusSeconds(3600))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = "shared-replace"
                execute {
                    replaceStarted.complete(executionId)
                    awaitCancellation()
                }
            }

            job("cancel-job") {
                at(Instant.now(clock).plusSeconds(3600))
                execute {
                    cancelStarted.complete(executionId)
                    awaitCancellation()
                }
            }
        }

        val scheduler = Scheduler(config, backgroundScope, clock)
        // Persist all definitions first and enqueue explicit test executions
        config.jobs.forEach { store.saveJob(it) }

        // Test initial seed true vs suppressed false
        val alreadySeededJob = JobDefinition(
            id = "already-seeded",
            handler = {},
            trigger = OneTimeTrigger(Instant.EPOCH)
        )
        suppressInitialSeedForJob.set("already-seeded")
        scheduler.registerJob(alreadySeededJob)
        assertFalse(scheduledEvents.any { it.first == "already-seeded" })

        val freshSeedJob = JobDefinition(
            id = "fresh-seeded",
            handler = {},
            trigger = OneTimeTrigger(Instant.EPOCH)
        )
        scheduler.registerJob(freshSeedJob)
        assertTrue(scheduledEvents.contains("fresh-seeded" to ScheduleReason.INITIAL_SEED))

        // 1. Trigger manual execution for lag >= 5s check (MisfirePolicy.FIRE_NOW default)
        val lagExec = JobExecution(
            jobId = "interval-ok",
            scheduledAt = Instant.now(clock).minusSeconds(6),
            lockKey = "interval-ok"
        )
        store.saveExecution(lagExec)
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()

        assertEquals("trace-interval-ok-0", observedContextTag.get())
        val startedLag = startedEvents.first { it.first.executionId == lagExec.id }.second
        assertTrue(startedLag >= Duration.ofSeconds(6))
        assertTrue(completedEvents.any { it.first.executionId == lagExec.id && it.second == ExecutionOutcome.SUCCESS && it.third == null })
        assertTrue(scheduledEvents.contains("interval-ok" to ScheduleReason.RECURRING_NEXT))

        // 2. Clock skew clamping -> lag == Duration.ZERO when scheduledAt > now
        val skewExec = JobExecution(
            jobId = "skew-job",
            scheduledAt = Instant.now(clock).minusSeconds(1)
        )
        store.saveExecution(skewExec)
        forceZeroLagNextPoll.set(true)
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        val skewStartedLag = startedEvents.first { it.first.executionId == skewExec.id }.second
        assertEquals(Duration.ZERO, skewStartedLag)

        // 3. Dead-letter with null exception message -> persists "IllegalStateException"
        val dlqExec = JobExecution(jobId = "null-msg-dlq", scheduledAt = Instant.now(clock))
        store.saveExecution(dlqExec)
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        val storedDlq = store.getExecution(dlqExec.id)
        assertEquals(ExecutionStatus.DEAD_LETTERED, storedDlq?.status)
        assertEquals("IllegalStateException", storedDlq?.error)
        assertTrue(
            completedEvents.any {
                it.first.executionId == dlqExec.id &&
                    it.second == ExecutionOutcome.DEAD_LETTERED &&
                    it.third == "IllegalStateException"
            }
        )

        // 4. Retry + ErrorDiagnosticFormatter redaction + MANUAL_TRIGGER
        scheduler.trigger("flaky-job")
        assertTrue(scheduledEvents.contains("flaky-job" to ScheduleReason.MANUAL_TRIGGER))
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        assertTrue(retryEvents.any { it.first == "flaky-job" && it.second == 1 && it.third == "IllegalArgumentException" })
        assertTrue(scheduledEvents.contains("flaky-job" to ScheduleReason.RETRY))
        assertTrue(
            completedEvents.any {
                it.first.jobId == "flaky-job" &&
                    it.second == ExecutionOutcome.FAILED_RETRY &&
                    it.third == "IllegalArgumentException"
            }
        )
        val failedAttempt0 = store.listExecutions(JobExecutionQuery(jobId = "flaky-job"))
            .first { it.status == ExecutionStatus.FAILED }
        assertTrue(failedAttempt0.error?.contains("[REDACTED]") == true)
        assertFalse(failedAttempt0.error?.contains("secret-token-123") == true)

        advanceTimeBy(150.milliseconds)
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        assertTrue(completedEvents.any { it.first.jobId == "flaky-job" && it.second == ExecutionOutcome.SUCCESS })

        // 5. MISFIRED outcome
        val misfiredExec = JobExecution(
            jobId = "misfire-ignore",
            scheduledAt = Instant.now(clock).minusSeconds(10)
        )
        store.saveExecution(misfiredExec)
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        assertTrue(
            completedEvents.any {
                it.first.executionId == misfiredExec.id &&
                    it.second == ExecutionOutcome.MISFIRED &&
                    it.third == null
            }
        )

        // 6. SUPERSEDED outcome (emitted once for superseded claim)
        val firstReplaceId = scheduler.startJob("replace-job").affectedExecutionIds.single()
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        assertEquals(firstReplaceId, replaceStarted.await())
        scheduler.trigger("replace-job")
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        val supersededForFirst = completedEvents.filter { it.first.executionId == firstReplaceId }
        assertEquals(1, supersededForFirst.size)
        assertEquals(ExecutionOutcome.SUPERSEDED, supersededForFirst.single().second)

        // 7. CANCELLED outcome via stopExecution (emitted once)
        val cancelId = scheduler.startJob("cancel-job").affectedExecutionIds.single()
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        assertEquals(cancelId, cancelStarted.await())
        assertTrue(scheduler.stopExecution(cancelId).success)
        runCurrent()
        val cancelCompletions = completedEvents.filter { it.first.executionId == cancelId }
        assertEquals(1, cancelCompletions.size)
        assertEquals(ExecutionOutcome.CANCELLED, cancelCompletions.single().second)

        scheduler.stop()

        // 8. SHUTDOWN_CHECKPOINT outcome (emitted once)
        val cpStore = MockJobStore(clock)
        val cpOutcomes = CopyOnWriteArrayList<ExecutionOutcome>()
        val cpEntered = CompletableDeferred<Unit>()
        val cpProceed = CompletableDeferred<Unit>()
        val cpScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = cpStore
                pollingInterval = Duration.ofMillis(50)
                shutdownTimeout = Duration.ofSeconds(2)
                this.observer = object : KhronaObserver {
                    override fun onExecutionCompleted(
                        context: ExecutionContext,
                        outcome: ExecutionOutcome,
                        duration: Duration,
                        errorType: String?
                    ) {
                        cpOutcomes.add(outcome)
                    }
                }
                job("cp-obs-job") {
                    once()
                    execute {
                        cpEntered.complete(Unit)
                        cpProceed.await()
                        checkpoint()
                    }
                }
            },
            backgroundScope,
            clock
        )
        cpStore.saveJob(cpScheduler.config.jobs.single())
        cpStore.saveExecution(JobExecution(jobId = "cp-obs-job", scheduledAt = Instant.EPOCH))
        cpScheduler.pollAndExecute()
        runCurrent()
        cpEntered.await()
        val stopDeferred = async { cpScheduler.stop() }
        runCurrent()
        cpProceed.complete(Unit)
        stopDeferred.await()
        assertEquals(listOf(ExecutionOutcome.SHUTDOWN_CHECKPOINT), cpOutcomes)
    }

    @Test
    fun `Scheduler tolerates throwing observer callbacks and broken custom ErrorDiagnosticFormatter`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val siblingCompleted = AtomicInteger(0)

        val throwingObserver = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                throw IllegalStateException("around before failure")
            }

            override fun onExecutionScheduled(jobId: String, reason: ScheduleReason) = error("scheduled boom")
            override fun onExecutionStarted(context: ExecutionContext, lag: Duration) = error("started boom")
            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) = error("completed boom")
            override fun onRetryScheduled(
                context: ExecutionContext,
                nextAttempt: Int,
                delay: Duration,
                errorType: String
            ) = error("retry boom")
            override fun onHeartbeatSucceeded(context: ExecutionContext) = error("hb ok boom")
            override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) = error("hb fail boom")
            override fun onStaleExecutionsRecovered(count: Int) = error("recovery boom")
            override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) = error("lock boom")
            override fun onDeadLetterCountSampled(count: Long) = error("dlq boom")
            override fun onRetentionCleanup(deletedCount: Int) = error("cleanup boom")
            override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) = error("shutdown boom")
        }
        val siblingObserver = object : KhronaObserver {
            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                siblingCompleted.incrementAndGet()
            }
        }

        val config = KhronaConfig().apply {
            this.store = store
            addObserver(throwingObserver)
            addObserver(siblingObserver)
            errorFormatter = ErrorDiagnosticFormatter { throw RuntimeException("broken formatter") }
            pollingInterval = Duration.ofMillis(100)
            executionLeaseDuration = Duration.ofSeconds(1)
            heartbeatInterval = Duration.ofMillis(200)
            job("resilient-dlq") {
                once()
                retry { maxAttempts = 1 }
                execute {
                    throw IllegalStateException("secret-token=999")
                }
            }
        }
        val scheduler = Scheduler(config, backgroundScope, clock)
        config.jobs.forEach { store.saveJob(it) }
        val exec = JobExecution(jobId = "resilient-dlq", scheduledAt = Instant.EPOCH)
        store.saveExecution(exec)

        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()
        scheduler.stop()

        val saved = store.getExecution(exec.id)
        assertEquals(ExecutionStatus.DEAD_LETTERED, saved?.status)
        assertEquals("IllegalStateException", saved?.error)
        assertFalse(saved?.error?.contains("secret-token") == true)
        assertEquals(1, siblingCompleted.get())
    }

    @Test
    fun `Scheduler emits heartbeat stale recovery lock contention DLQ sampling retention cleanup and shutdown events`() = runBlocking {
        val baseStore = MockJobStore()
        val hbMode = AtomicReference("SUCCEED") // SUCCEED | FALSE | THROW_ONCE_THEN_SUCCEED | HANG
        val lockContentionMode = AtomicBoolean(false)
        val forcePostClaimContention = AtomicBoolean(false)
        val recoveredToReturn = AtomicInteger(2)
        val cleanedToReturn = AtomicInteger(3)

        val store = object : JobStore by baseStore {
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
                return when (hbMode.get()) {
                    "SUCCEED" -> baseStore.heartbeat(id, workerId, leaseDuration)
                    "FALSE" -> false
                    "THROW_ONCE_THEN_SUCCEED" -> {
                        hbMode.set("SUCCEED")
                        throw RuntimeException("transient hb store failure")
                    }
                    "HANG" -> {
                        delay(5_000)
                        true
                    }
                    else -> false
                }
            }

            override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean {
                if (forcePostClaimContention.get() && excludeExecutionId != null) {
                    return true
                }
                if (excludeExecutionId == null && lockContentionMode.get()) {
                    return true
                }
                return baseStore.isLockHeld(lockKey, excludeExecutionId)
            }

            override suspend fun resetExpiredExecutions(now: Instant): Int =
                recoveredToReturn.getAndSet(0)

            override suspend fun cleanupCompletedExecutions(
                before: Instant,
                statuses: Set<ExecutionStatus>,
                limit: Int
            ): Int = cleanedToReturn.getAndSet(0)
        }

        val hbSuccesses = AtomicInteger(0)
        val hbFailures = CopyOnWriteArrayList<HeartbeatFailureReason>()
        val staleRecoveredCounts = CopyOnWriteArrayList<Int>()
        val lockContentions = CopyOnWriteArrayList<Triple<String, String, ConcurrencyPolicy>>()
        val dlqSamples = CopyOnWriteArrayList<Long>()
        val cleanupCounts = CopyOnWriteArrayList<Int>()
        val shutdownEvents = CopyOnWriteArrayList<Triple<Duration, Boolean, Int>>()

        val observer = object : KhronaObserver {
            override fun onHeartbeatSucceeded(context: ExecutionContext) {
                hbSuccesses.incrementAndGet()
            }

            override fun onHeartbeatFailed(context: ExecutionContext, reason: HeartbeatFailureReason) {
                hbFailures.add(reason)
            }

            override fun onStaleExecutionsRecovered(count: Int) {
                staleRecoveredCounts.add(count)
            }

            override fun onLockContention(jobId: String, lockKey: String, policy: ConcurrencyPolicy) {
                lockContentions.add(Triple(jobId, lockKey, policy))
            }

            override fun onDeadLetterCountSampled(count: Long) {
                dlqSamples.add(count)
            }

            override fun onRetentionCleanup(deletedCount: Int) {
                cleanupCounts.add(deletedCount)
            }

            override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {
                assertFalse(duration.isNegative)
                shutdownEvents.add(Triple(duration, clean, remainingActiveCount))
            }
        }

        val hbJobGate = AtomicReference<CompletableDeferred<Unit>?>(null)
        val leaseExpiredSeen = CompletableDeferred<Unit>()
        val lostOwnershipSeen = CompletableDeferred<Unit>()
        val config = KhronaConfig().apply {
            this.store = store
            this.observer = observer
            pollingInterval = Duration.ofMillis(50)
            executionLeaseDuration = Duration.ofMillis(120)
            heartbeatInterval = Duration.ofMillis(40)
            deadLetterSamplingInterval = Duration.ofMillis(50)
            retention {
                enabled = true
                cleanupInterval(50.milliseconds)
                cleanupJitter = 0.0
            }
            shutdownTimeout = Duration.ofMillis(800)

            job("hb-job") {
                at(Instant.now().plusSeconds(3600))
                execute {
                    hbJobGate.get()?.await()
                }
            }

            job("hb-lost-job") {
                at(Instant.now().plusSeconds(3600))
                execute {
                    try {
                        awaitCancellation()
                    } finally {
                        lostOwnershipSeen.complete(Unit)
                    }
                }
            }

            job("hb-hang-job") {
                at(Instant.now().plusSeconds(3600))
                execute {
                    try {
                        awaitCancellation()
                    } finally {
                        leaseExpiredSeen.complete(Unit)
                    }
                }
            }

            job("forbid-job") {
                at(Instant.now().plusSeconds(3600))
                concurrencyPolicy = ConcurrencyPolicy.FORBID
                lockKey = "forbid-lock"
                execute {}
            }
        }

        // Seed 2 existing dead-lettered rows for DLQ gauge sampling
        store.saveExecution(JobExecution(jobId = "dlq-a", scheduledAt = Instant.EPOCH, status = ExecutionStatus.DEAD_LETTERED))
        store.saveExecution(JobExecution(jobId = "dlq-b", scheduledAt = Instant.EPOCH, status = ExecutionStatus.DEAD_LETTERED))

        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler = Scheduler(config, scope)
        try {
            scheduler.start()
            withTimeout(3_000) {
                while (staleRecoveredCounts.isEmpty() || cleanupCounts.isEmpty() || dlqSamples.isEmpty()) {
                    delay(20)
                }
            }
            assertEquals(listOf(2), staleRecoveredCounts)
            assertEquals(listOf(3), cleanupCounts)
            assertTrue(dlqSamples.last() == 2L)

            // Pre-claim FORBID lock contention
            lockContentionMode.set(true)
            store.saveExecution(JobExecution(jobId = "forbid-job", scheduledAt = Instant.now().minusSeconds(1), lockKey = "forbid-lock"))
            withTimeout(3_000) {
                while (lockContentions.isEmpty()) delay(20)
            }
            assertEquals(Triple("forbid-job", "forbid-lock", ConcurrencyPolicy.FORBID), lockContentions.first())

            // Post-claim FORBID lock contention (arm post-claim check BEFORE releasing pre-claim check)
            val prevContentions = lockContentions.size
            forcePostClaimContention.set(true)
            lockContentionMode.set(false)
            withTimeout(3_000) {
                while (lockContentions.size <= prevContentions) delay(20)
            }
            forcePostClaimContention.set(false)

            // Heartbeat STORE_ERROR + SUCCEED
            val gate1 = CompletableDeferred<Unit>()
            hbJobGate.set(gate1)
            hbMode.set("THROW_ONCE_THEN_SUCCEED")
            scheduler.startJob("hb-job")
            withTimeout(3_000) {
                while (!hbFailures.contains(HeartbeatFailureReason.STORE_ERROR) || hbSuccesses.get() < 1) {
                    delay(20)
                }
            }
            assertFalse(hbFailures.contains(HeartbeatFailureReason.LOST_OWNERSHIP_OR_EXPIRED))
            gate1.complete(Unit)

            // Heartbeat LOST_OWNERSHIP_OR_EXPIRED
            hbMode.set("FALSE")
            scheduler.startJob("hb-lost-job")
            withTimeout(3_000) { lostOwnershipSeen.await() }
            assertTrue(hbFailures.contains(HeartbeatFailureReason.LOST_OWNERSHIP_OR_EXPIRED))

            // Heartbeat LEASE_BUDGET_EXCEEDED
            hbMode.set("HANG")
            scheduler.startJob("hb-hang-job")
            withTimeout(3_000) { leaseExpiredSeen.await() }
            assertTrue(hbFailures.contains(HeartbeatFailureReason.LEASE_BUDGET_EXCEEDED))

            scheduler.stop()
            assertEquals(1, shutdownEvents.size)
            assertEquals(true, shutdownEvents.single().second)
            assertEquals(0, shutdownEvents.single().third)
        } finally {
            scope.cancel()
        }

        // Forced timeout shutdown emits clean = false and remainingActiveCount >= 1
        val ioScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        val startedLatch = java.util.concurrent.CountDownLatch(1)
        val releaseLatch = java.util.concurrent.CountDownLatch(1)
        val forcedShutdownEvents = CopyOnWriteArrayList<Pair<Boolean, Int>>()
        val forcedScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = MockJobStore()
                shutdownTimeout = Duration.ofMillis(80)
                this.observer = object : KhronaObserver {
                    override fun onShutdownCompleted(duration: Duration, clean: Boolean, remainingActiveCount: Int) {
                        forcedShutdownEvents.add(clean to remainingActiveCount)
                    }
                }
                job("blocking-shutdown") {
                    once()
                    execute {
                        startedLatch.countDown()
                        releaseLatch.await(5, java.util.concurrent.TimeUnit.SECONDS)
                    }
                }
            },
            ioScope
        )
        try {
            forcedScheduler.config.store!!.saveJob(forcedScheduler.config.jobs.single())
            forcedScheduler.config.store!!.saveExecution(
                JobExecution(jobId = "blocking-shutdown", scheduledAt = Instant.EPOCH)
            )
            forcedScheduler.pollAndExecute()
            assertTrue(startedLatch.await(2, java.util.concurrent.TimeUnit.SECONDS))
            forcedScheduler.stop()
            assertEquals(1, forcedShutdownEvents.size)
            assertFalse(forcedShutdownEvents.single().first)
            assertTrue(forcedShutdownEvents.single().second >= 1)
        } finally {
            releaseLatch.countDown()
            ioScope.cancel()
        }
    }

    @Test
    fun `Scheduler health transitions across STOPPED STARTING UP DEGRADED DOWN and recovery`(): Unit = runBlocking {
        val currentNow = AtomicReference(Instant.parse("2030-01-01T00:00:00Z"))
        val mutableClock = object : Clock() {
            override fun getZone(): ZoneId = ZoneId.of("UTC")
            override fun withZone(zone: ZoneId?): Clock = this
            override fun instant(): Instant = currentNow.get()
        }
        val baseStore = MockJobStore(mutableClock)
        val storeHealthy = AtomicBoolean(true)
        val failNextPolls = AtomicInteger(0)
        val startupGate = CompletableDeferred<Unit>()
        val useStartupGate = AtomicBoolean(true)

        val store = object : JobStore by baseStore {
            override suspend fun checkHealth(): StoreHealth {
                return if (storeHealthy.get()) {
                    StoreHealth(HealthStatus.UP, Duration.ofMillis(1))
                } else {
                    StoreHealth(HealthStatus.DOWN, Duration.ofMillis(2), errorType = "ConnectException", detail = "refused")
                }
            }

            override suspend fun listEligibleExecutions(
                now: Instant,
                limit: Int,
                jobIds: Set<String>?
            ): List<JobExecution> {
                if (useStartupGate.get()) {
                    startupGate.await()
                }
                if (failNextPolls.get() > 0) {
                    failNextPolls.decrementAndGet()
                    throw IllegalStateException("transient poll failure")
                }
                return baseStore.listEligibleExecutions(now, limit, jobIds)
            }
        }

        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val jobRan = CompletableDeferred<Unit>()
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                pollingInterval = Duration.ofMillis(40)
                healthPollFailureThreshold = 2
                shutdownTimeout = Duration.ofMillis(500)
                job("health-job") {
                    once()
                    execute { jobRan.complete(Unit) }
                }
            },
            scope,
            mutableClock
        )

        try {
            // 1. Before start() -> STOPPED
            val beforeStart = scheduler.health()
            assertEquals(HealthStatus.STOPPED, beforeStart.status)
            assertFalse(beforeStart.isHealthy)
            assertFalse(beforeStart.isReady)

            // 2. Immediately after start() before first poll completes -> STARTING
            scheduler.start()
            val starting = scheduler.health()
            assertEquals(HealthStatus.STARTING, starting.status)
            assertFalse(starting.isHealthy)
            assertFalse(starting.isReady)

            // Allow first poll to complete -> UP
            useStartupGate.set(false)
            startupGate.complete(Unit)
            withTimeout(3_000) { jobRan.await() }
            withTimeout(3_000) {
                while (scheduler.health().status != HealthStatus.UP) delay(15)
            }
            val up = scheduler.health()
            assertEquals(HealthStatus.UP, up.status)
            assertTrue(up.isHealthy)
            assertTrue(up.isReady)
            assertEquals(0, up.consecutivePollFailures)
            assertNull(up.lastPollErrorType)
            assertNotNull(up.lastPollAt)
            assertNotNull(up.lastSuccessfulPollAt)
        } finally {
            scope.cancel()
        }

        // Deterministic poll-failure threshold & stall test via poll-controlled store
        val pollGate = AtomicReference<CompletableDeferred<Unit>?>(null)
        val failNextSinglePollThenPause = AtomicReference<CompletableDeferred<Unit>?>(null)
        val failEveryPoll = AtomicBoolean(false)
        val failSaveJob = AtomicBoolean(true)
        val recoveredJobRan = CompletableDeferred<Unit>()
        val controlledClockNow = AtomicReference(Instant.parse("2030-01-01T00:00:00Z"))
        val controlledClock = object : Clock() {
            override fun getZone(): ZoneId = ZoneId.of("UTC")
            override fun withZone(zone: ZoneId?): Clock = this
            override fun instant(): Instant = controlledClockNow.get()
        }
        val controlledBase = MockJobStore(controlledClock)
        val controlledStore = object : JobStore by controlledBase {
            override suspend fun checkHealth(): StoreHealth {
                return if (storeHealthy.get()) {
                    StoreHealth(HealthStatus.UP, Duration.ofMillis(1))
                } else {
                    throw IllegalStateException("health ping crashed")
                }
            }

            override suspend fun getJob(jobId: String): JobDefinition? {
                if (failSaveJob.get()) throw IllegalStateException("startup outage")
                return controlledBase.getJob(jobId)
            }

            override suspend fun listEligibleExecutions(
                now: Instant,
                limit: Int,
                jobIds: Set<String>?
            ): List<JobExecution> {
                pollGate.get()?.await()
                val singlePauseGate = failNextSinglePollThenPause.getAndSet(null)
                if (singlePauseGate != null) {
                    pollGate.set(singlePauseGate)
                    throw IllegalStateException("single poll failure")
                }
                if (failEveryPoll.get()) throw IllegalStateException("poll failure")
                return controlledBase.listEligibleExecutions(now, limit, jobIds)
            }
        }

        val scope2 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val s2 = Scheduler(
            KhronaConfig().apply {
                this.store = controlledStore
                pollingInterval = Duration.ofMillis(30)
                healthPollFailureThreshold = 2
                shutdownTimeout = Duration.ofMillis(500)
                job("startup-recovery-job") {
                    once()
                    execute { recoveredJobRan.complete(Unit) }
                }
            },
            scope2,
            controlledClock
        )

        try {
            // Startup outage: store unavailable at start -> loop survives, health is DOWN, then auto-recovers
            storeHealthy.set(false)
            s2.start()
            withTimeout(3_000) {
                while (s2.health().consecutivePollFailures < 1) delay(15)
            }
            val downStartup = s2.health()
            assertEquals(HealthStatus.DOWN, downStartup.status)
            assertEquals(HealthStatus.DOWN, downStartup.storeHealth.status)
            assertEquals("IllegalStateException", downStartup.storeHealth.errorType)
            assertFalse(downStartup.isHealthy)
            assertFalse(downStartup.isReady)

            // Store recovers -> startup job initializes and runs -> UP
            storeHealthy.set(true)
            failSaveJob.set(false)
            withTimeout(3_000) { recoveredJobRan.await() }
            withTimeout(3_000) {
                while (s2.health().status != HealthStatus.UP) delay(15)
            }

            // Force exactly 1 poll failure then pause subsequent polls -> DEGRADED
            val resumeAfterSingleFailure = CompletableDeferred<Unit>()
            failNextSinglePollThenPause.set(resumeAfterSingleFailure)
            withTimeout(3_000) {
                while (s2.health().consecutivePollFailures != 1) delay(10)
            }
            val degradedSingleFailure = s2.health()
            assertEquals(HealthStatus.DEGRADED, degradedSingleFailure.status)
            assertTrue(degradedSingleFailure.isHealthy)
            assertFalse(degradedSingleFailure.isReady)
            assertEquals(1, degradedSingleFailure.consecutivePollFailures)
            assertEquals("IllegalStateException", degradedSingleFailure.lastPollErrorType)

            // Allow polls to fail repeatedly to hit threshold (2) -> DOWN even when store checkHealth() is UP
            failEveryPoll.set(true)
            pollGate.set(null)
            resumeAfterSingleFailure.complete(Unit)
            withTimeout(3_000) {
                while (s2.health().consecutivePollFailures < 2) delay(15)
            }
            val downByThreshold = s2.health()
            assertEquals(HealthStatus.DOWN, downByThreshold.status)
            assertEquals(HealthStatus.UP, downByThreshold.storeHealth.status)
            assertFalse(downByThreshold.isHealthy)
            assertFalse(downByThreshold.isReady)

            // Recover polls -> UP
            failEveryPoll.set(false)
            withTimeout(3_000) {
                while (s2.health().status != HealthStatus.UP) delay(15)
            }

            // Poll loop stall while store is UP -> DEGRADED
            val freezeGate = CompletableDeferred<Unit>()
            pollGate.set(freezeGate)
            controlledClockNow.set(controlledClockNow.get().plusSeconds(10))
            val stalledHealth = s2.health()
            assertEquals(HealthStatus.DEGRADED, stalledHealth.status)
            assertTrue(stalledHealth.isHealthy)
            assertFalse(stalledHealth.isReady)
            pollGate.set(null)
            freezeGate.complete(Unit)

            // stop() -> STOPPED
            s2.stop()
            val afterStop = s2.health()
            assertEquals(HealthStatus.STOPPED, afterStop.status)
            assertFalse(afterStop.isHealthy)
            assertFalse(afterStop.isReady)
        } finally {
            scope2.cancel()
        }

        // Cover pollingInterval * 3 > 5s stall window (3s * 3 = 9s)
        val stallScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val stallClockNow = AtomicReference(Instant.parse("2030-01-01T00:00:00Z"))
        val stallClock = object : Clock() {
            override fun getZone(): ZoneId = ZoneId.of("UTC")
            override fun withZone(zone: ZoneId?): Clock = this
            override fun instant(): Instant = stallClockNow.get()
        }
        val stallStore = MockJobStore(stallClock)
        val stallScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = stallStore
                pollingInterval = Duration.ofSeconds(3)
                shutdownTimeout = Duration.ofMillis(200)
            },
            stallScope,
            stallClock
        )
        try {
            stallScheduler.start()
            withTimeout(3_000) {
                while (stallScheduler.health().status != HealthStatus.UP) delay(15)
            }
            val basePollTime = stallScheduler.health().lastPollAt!!
            stallClockNow.set(basePollTime.plusSeconds(6))
            assertEquals(HealthStatus.UP, stallScheduler.health().status)
            stallClockNow.set(basePollTime.plusSeconds(10))
            assertEquals(HealthStatus.DEGRADED, stallScheduler.health().status)
            stallScheduler.stop()
        } finally {
            stallScope.cancel()
        }
    }

    @Test
    fun `DLQ sampling skips NO_OP observer and isolates countExecutions failures without blocking polling`() = runBlocking {
        // 1. Default NO_OP observer skips countExecutions completely
        val noopBase = MockJobStore()
        val noopCountCalls = AtomicInteger(0)
        val noopJobRan = CompletableDeferred<Unit>()
        val noopStore = object : JobStore by noopBase {
            override suspend fun countExecutions(query: JobExecutionQuery): Long {
                noopCountCalls.incrementAndGet()
                throw IllegalStateException("countExecutions should not run when observer is NO_OP")
            }
        }
        val scope1 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler1 = Scheduler(
            KhronaConfig().apply {
                this.store = noopStore
                pollingInterval = Duration.ofMillis(30)
                deadLetterSamplingInterval = Duration.ofMillis(30)
                shutdownTimeout = Duration.ofMillis(500)
                job("noop-dlq-job") {
                    once()
                    execute { noopJobRan.complete(Unit) }
                }
            },
            scope1
        )
        try {
            scheduler1.start()
            withTimeout(3_000) { noopJobRan.await() }
            delay(80)
            assertEquals(0, noopCountCalls.get())
            assertEquals(HealthStatus.UP, scheduler1.health().status)
            scheduler1.stop()
        } finally {
            scope1.cancel()
        }

        // 2. Active observer with failing countExecutions still polls jobs and advances sampling cadence
        val activeBase = MockJobStore()
        val activeCountCalls = AtomicInteger(0)
        val activeJobRuns = AtomicInteger(0)
        val activeStore = object : JobStore by activeBase {
            override suspend fun countExecutions(query: JobExecutionQuery): Long {
                activeCountCalls.incrementAndGet()
                throw IllegalStateException("DLQ count failed")
            }
        }
        val scope2 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler2 = Scheduler(
            KhronaConfig().apply {
                this.store = activeStore
                observer = object : KhronaObserver {}
                pollingInterval = Duration.ofMillis(30)
                deadLetterSamplingInterval = Duration.ofMinutes(10)
                shutdownTimeout = Duration.ofMillis(500)
                job("active-dlq-job") {
                    every(Duration.ofMillis(40))
                    execute { activeJobRuns.incrementAndGet() }
                }
            },
            scope2
        )
        try {
            scheduler2.start()
            withTimeout(3_000) {
                while (activeJobRuns.get() < 2) delay(15)
            }
            assertEquals(1, activeCountCalls.get(), "Failed DLQ sample must not retry on every poll tick")
            assertEquals(HealthStatus.UP, scheduler2.health().status)
            scheduler2.stop()
        } finally {
            scope2.cancel()
        }
    }

    @Test
    fun `observer throwing CancellationException in synchronous callbacks does not stop scheduler`() = runBlocking {
        val store = MockJobStore()
        val jobRan = CompletableDeferred<Unit>()
        val siblingSawSample = AtomicBoolean(false)
        val siblingSawCompleted = AtomicBoolean(false)

        val cancellingObserver = object : KhronaObserver {
            override fun onDeadLetterCountSampled(count: Long) {
                throw CancellationException("observer cancelled on DLQ sample")
            }

            override fun onExecutionStarted(context: ExecutionContext, lag: Duration) {
                throw CancellationException("observer cancelled on start")
            }

            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                throw CancellationException("observer cancelled on complete")
            }
        }
        val siblingObserver = object : KhronaObserver {
            override fun onDeadLetterCountSampled(count: Long) {
                siblingSawSample.set(true)
            }

            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                siblingSawCompleted.set(true)
            }
        }

        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                addObserver(cancellingObserver)
                addObserver(siblingObserver)
                pollingInterval = Duration.ofMillis(30)
                deadLetterSamplingInterval = Duration.ofMillis(30)
                shutdownTimeout = Duration.ofMillis(500)
                job("cancel-safe-job") {
                    once()
                    execute { jobRan.complete(Unit) }
                }
            },
            scope
        )
        try {
            scheduler.start()
            withTimeout(3_000) { jobRan.await() }
            withTimeout(3_000) {
                while (!siblingSawSample.get() || !siblingSawCompleted.get()) delay(15)
            }
            assertEquals(HealthStatus.UP, scheduler.health().status)
            scheduler.stop()
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `aroundExecution cannot invoke handler twice or hide handler failure`() = runTest {
        val ctx = sampleContext()

        // 1. Retrying aroundExecution must NOT run handler twice and must preserve original failure
        val retryAfterFailCount = AtomicInteger(0)
        val retryingObserver = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                return try {
                    block()
                } catch (_: Throwable) {
                    block()
                }
            }
        }
        val originalError = IllegalStateException("first-attempt-failed")
        val thrown = assertThrows(IllegalStateException::class.java) {
            runBlocking<Unit> {
                invokeAroundSafely(retryingObserver, ctx) {
                    if (retryAfterFailCount.incrementAndGet() == 1) {
                        throw originalError
                    }
                    "hidden-success"
                }
            }
        }
        assertSame(originalError, thrown)
        assertEquals(1, retryAfterFailCount.get())

        // 2. Swallowing aroundExecution must still rethrow original failure
        val swallowingObserver = object : KhronaObserver {
            @Suppress("UNCHECKED_CAST")
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                return try {
                    block()
                } catch (_: Throwable) {
                    "swallowed" as T
                }
            }
        }
        val swallowedThrown = assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                invokeAroundSafely(swallowingObserver, ctx) {
                    throw IllegalArgumentException("must-not-be-hidden")
                }
            }
        }
        assertEquals("must-not-be-hidden", swallowedThrown.message)

        // 3. Double-invoking on success runs handler once and returns original result
        val doubleSuccessCount = AtomicInteger(0)
        val doubleCallObserver = object : KhronaObserver {
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                val first = block()
                block()
                return first
            }
        }
        val okResult = invokeAroundSafely(doubleCallObserver, ctx) {
            "run-${doubleSuccessCount.incrementAndGet()}"
        }
        assertEquals("run-1", okResult)
        assertEquals(1, doubleSuccessCount.get())

        // 4. Short-circuiting aroundExecution (never invokes block) still executes handler once
        val shortCircuitCount = AtomicInteger(0)
        val shortCircuitObserver = object : KhronaObserver {
            @Suppress("UNCHECKED_CAST")
            override suspend fun <T> aroundExecution(context: ExecutionContext, block: suspend () -> T): T {
                return "skipped" as T
            }
        }
        val directResult = invokeAroundSafely(shortCircuitObserver, ctx) {
            "direct-${shortCircuitCount.incrementAndGet()}"
        }
        assertEquals("direct-1", directResult)
        assertEquals(1, shortCircuitCount.get())

        // 5. End-to-end with Scheduler: retrying aroundExecution cannot mask handler failure from retry accounting
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val handlerRuns = AtomicInteger(0)
        val outcomes = CopyOnWriteArrayList<ExecutionOutcome>()
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                addObserver(retryingObserver)
                addObserver(object : KhronaObserver {
                    override fun onExecutionCompleted(
                        context: ExecutionContext,
                        outcome: ExecutionOutcome,
                        duration: Duration,
                        errorType: String?
                    ) {
                        outcomes.add(outcome)
                    }
                })
                job("no-double-run") {
                    once()
                    retry { maxAttempts = 2; initialDelay = Duration.ofSeconds(10) }
                    execute {
                        if (handlerRuns.incrementAndGet() == 1) {
                            throw IllegalStateException("boom-once")
                        }
                    }
                }
            },
            backgroundScope,
            clock
        )
        store.saveJob(scheduler.config.jobs.single())
        store.saveExecution(JobExecution(jobId = "no-double-run", scheduledAt = Instant.EPOCH))
        scheduler.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()

        assertEquals(1, handlerRuns.get())
        assertEquals(listOf(ExecutionOutcome.FAILED_RETRY), outcomes)
    }

    @Test
    fun `cross-node REPLACE emits SUPERSEDED once from replacing worker using store execution details`() = runTest {
        val clock = TestClock(testScheduler)
        val baseStore = MockJobStore(clock)
        val completed = CopyOnWriteArrayList<Triple<ExecutionContext, ExecutionOutcome, Duration>>()

        val observer = object : KhronaObserver {
            override fun onExecutionCompleted(
                context: ExecutionContext,
                outcome: ExecutionOutcome,
                duration: Duration,
                errorType: String?
            ) {
                completed.add(Triple(context, outcome, duration))
            }
        }

        // Seed remote worker A's RUNNING execution directly into the shared store (no local activeClaims on worker B)
        val remoteStartedAt = Instant.now(clock).minusSeconds(3)
        val remoteExec = JobExecution(
            id = UUID.randomUUID(),
            jobId = "cross-node-replace",
            status = ExecutionStatus.RUNNING,
            scheduledAt = remoteStartedAt.minusSeconds(1),
            startedAt = remoteStartedAt,
            attempt = 2,
            lockKey = "shared-cross-lock",
            workerId = "remote-worker-A:claim-1",
            expiresAt = Instant.now(clock).plusSeconds(60),
            correlationId = "remote-corr-A"
        )
        val replacementExec = JobExecution(
            id = UUID.randomUUID(),
            jobId = "cross-node-replace",
            status = ExecutionStatus.PENDING,
            scheduledAt = Instant.now(clock),
            lockKey = "shared-cross-lock"
        )

        val workerB = Scheduler(
            KhronaConfig().apply {
                this.store = baseStore
                this.observer = observer
                job("cross-node-replace") {
                    at(Instant.now(clock).plusSeconds(3600))
                    concurrencyPolicy = ConcurrencyPolicy.REPLACE
                    lockKey = "shared-cross-lock"
                    execute {}
                }
            },
            backgroundScope,
            clock
        )
        baseStore.saveJob(workerB.config.jobs.single())
        baseStore.saveExecution(remoteExec)
        baseStore.saveExecution(replacementExec)

        workerB.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()

        val superseded = completed.filter { it.second == ExecutionOutcome.SUPERSEDED }
        assertEquals(1, superseded.size)
        val (supersededCtx, _, elapsed) = superseded.single()
        assertEquals(remoteExec.id, supersededCtx.executionId)
        assertEquals("cross-node-replace", supersededCtx.jobId)
        assertEquals(2, supersededCtx.attempt)
        assertEquals(remoteExec.scheduledAt, supersededCtx.scheduledAt)
        assertEquals("remote-corr-A", supersededCtx.correlationId)
        assertTrue(elapsed >= Duration.ofSeconds(3))

        // Fallback when store.getExecution(supersededId) throws or returns null still emits SUPERSEDED
        val missingId = UUID.randomUUID()
        val throwingId = UUID.randomUUID()
        val fallbackCompleted = CopyOnWriteArrayList<ExecutionContext>()
        val fallbackBase = MockJobStore(clock)
        val fallbackStore = object : JobStore by fallbackBase {
            override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> =
                listOf(missingId, throwingId)

            override suspend fun getExecution(id: UUID): JobExecution? {
                if (id == throwingId) throw IllegalStateException("lookup failed")
                return fallbackBase.getExecution(id)
            }
        }
        val fallbackWorker = Scheduler(
            KhronaConfig().apply {
                this.store = fallbackStore
                this.observer = object : KhronaObserver {
                    override fun onExecutionCompleted(
                        context: ExecutionContext,
                        outcome: ExecutionOutcome,
                        duration: Duration,
                        errorType: String?
                    ) {
                        if (outcome == ExecutionOutcome.SUPERSEDED) {
                            fallbackCompleted.add(context)
                        }
                    }
                }
                job("fallback-replace") {
                    at(Instant.now(clock).plusSeconds(3600))
                    concurrencyPolicy = ConcurrencyPolicy.REPLACE
                    lockKey = "fallback-lock"
                    execute {}
                }
            },
            backgroundScope,
            clock
        )
        fallbackBase.saveJob(fallbackWorker.config.jobs.single())
        fallbackBase.saveExecution(JobExecution(jobId = "fallback-replace", scheduledAt = Instant.now(clock), lockKey = "fallback-lock"))
        fallbackWorker.pollAndExecute()
        advanceTimeBy(5.milliseconds)
        runCurrent()

        assertEquals(setOf(missingId, throwingId), fallbackCompleted.map { it.executionId }.toSet())
    }
}
