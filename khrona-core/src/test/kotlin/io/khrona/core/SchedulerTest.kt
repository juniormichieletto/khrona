@file:OptIn(ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class SchedulerTest {

    class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    @Test
    fun `scheduler should execute eligible jobs`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            job("test-job") {
                every(Duration.ofMinutes(1))
                execute {
                    // Job logic
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        // Setup an eligible execution
        val execution = JobExecution(jobId = "test-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        
        // Wait for polling and execution
        advanceTimeBy(5000)
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.SUCCESS, updated?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should handle job failure`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            job("fail-job") {
                every(Duration.ofMinutes(1))
                execute {
                    throw RuntimeException("Boom")
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        val execution = JobExecution(jobId = "fail-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        advanceTimeBy(5000)
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.FAILED, updated?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should retry failed jobs`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            job("retry-job") {
                every(Duration.ofMinutes(1))
                retry {
                    maxAttempts = 3
                    initialDelay = Duration.ofMillis(100)
                }
                execute {
                    throw RuntimeException("Temporary failure")
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        val execution = JobExecution(jobId = "retry-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        
        // First execution fails
        advanceTimeBy(5000)
        
        // Should have a new execution with attempt 1
        val retryExec = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 1 }
        assertNotNull(retryExec, "Retry execution should be scheduled")
        
        // Wait for retry to be eligible and executed
        advanceTimeBy(5000)
        
        // Should have attempt 2
        val retryExec2 = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 2 }
        assertNotNull(retryExec2, "Second retry execution should be scheduled")
        
        // Wait for second retry
        advanceTimeBy(5000)
        
        // Should be DEAD_LETTERED now as maxAttempts = 3 (0, 1, 2 are 3 attempts)
        val finalExec = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 2 }
        assertEquals(ExecutionStatus.DEAD_LETTERED, finalExec?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should fail to start if cron frequency is smaller than polling interval`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMinutes(5)
            job("invalid-cron-job") {
                cron("* * * * *") // 1m < 5m
                execute {}
            }
        }
        
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config, this, clock)
        }
    }

    @Test
    fun `scheduler should fail to start if job interval is smaller than polling interval`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofSeconds(5)
            job("invalid-job") {
                every(Duration.ofSeconds(1)) // 1s < 5s
                execute {}
            }
        }
        
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config, this, clock)
        }
    }

    @Test
    fun `scheduler should start if job interval is greater than or equal to polling interval`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(10)
            job("valid-job") {
                every(Duration.ofMillis(100))
                execute {}
            }
        }
        
        val scheduler = Scheduler(config, this, clock)
        assertDoesNotThrow {
            scheduler.start()
        }
        scheduler.stop()
    }

    @Test
    fun `registerJob should fail if job interval is smaller than polling interval`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofSeconds(5)
        }
        
        val scheduler = Scheduler(config, this, clock)
        val jobDef = JobBuilder("invalid-job").apply {
            every(Duration.ofSeconds(1))
            execute {}
        }.build()

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                scheduler.registerJob(jobDef)
            }
        }
    }

    @Test
    fun `registerJob should propagate store exceptions`() = runTest {
        val store = object : MockJobStore() {
            override suspend fun saveJob(job: JobDefinition) {
                throw RuntimeException("Store failure")
            }
        }
        val config = KhronaConfig().apply { this.store = store }
        val scheduler = Scheduler(config, this)
        
        val jobDef = JobBuilder("test-job").apply {
            once()
            execute {}
        }.build()
        
        assertThrows(RuntimeException::class.java) {
            runBlocking {
                scheduler.registerJob(jobDef)
            }
        }
    }

    @Test
    fun `trigger should propagate store exceptions`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply { this.store = store }
        val scheduler = Scheduler(config, this)
        
        val jobDef = JobBuilder("test-job").apply {
            once()
            execute {}
        }.build()
        
        scheduler.registerJob(jobDef)
        
        // Mock a failure for saveExecution
        val failingStore = object : MockJobStore() {
            override suspend fun saveExecution(execution: JobExecution) {
                throw RuntimeException("Store failure")
            }
            override suspend fun getJob(jobId: String): JobDefinition? = store.getJob(jobId)
        }
        val failingScheduler = Scheduler(config.apply { this.store = failingStore }, this)
        
        assertThrows(RuntimeException::class.java) {
            runBlocking {
                failingScheduler.trigger("test-job")
            }
        }
    }

    @Test
    fun `scheduler should execute one-time jobs`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        var executed = false
        val config = KhronaConfig().apply {
            this.store = store
            job("one-time-job") {
                once()
                execute {
                    executed = true
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        scheduler.start()
        
        // Wait for polling and execution
        advanceTimeBy(5000)
        
        assertTrue(executed, "Job should have been executed")
        
        // Check if no more executions are scheduled
        val executions = store.executions.values.filter { it.jobId == "one-time-job" }
        assertEquals(1, executions.size, "Should only have one execution")
        assertEquals(ExecutionStatus.SUCCESS, executions[0].status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should support manual trigger`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        var callCount = 0
        val config = KhronaConfig().apply {
            this.store = store
            job("manual-job") {
                at(Instant.now(clock).plus(Duration.ofDays(365 * 100))) // Never run automatically
                execute {
                    callCount++
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        scheduler.start()
        
        // Wait a bit for scheduler.start() to finish initial registration
        advanceTimeBy(100) 
        
        // Manually trigger
        scheduler.trigger("manual-job")
        
        advanceTimeBy(5000)
        
        assertEquals(1, callCount, "Job should have been executed once due to manual trigger")
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should propagate correlationId but keep it unique per recurring run`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val capturedIds = mutableListOf<String?>()
        
        val config = KhronaConfig().apply {
            this.store = store
            job("correlation-job") {
                every(Duration.ofSeconds(1))
                execute {
                    capturedIds.add(MDC.get("correlationId"))
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        // Manually set a correlationId in MDC before starting
        val originalCorrelationId = "registration-id"
        MDC.put("correlationId", originalCorrelationId)
        try {
            scheduler.start()
            
            // Wait for first execution (scheduled at T=1 because baseline is EPOCH and interval is 1s)
            advanceTimeBy(1500) 
            assertEquals(1, capturedIds.size)
            assertEquals(originalCorrelationId, capturedIds[0], "First execution should have registration correlationId")
            
            // Wait for second recurring execution (scheduled at T=2)
            advanceTimeBy(1000)
            assertEquals(2, capturedIds.size)
            assertNotEquals(originalCorrelationId, capturedIds[1], "Second recurring run should have a NEW unique correlationId")
            
            // Test trigger propagation
            val triggerCorrelationId = "trigger-id"
            MDC.put("correlationId", triggerCorrelationId)
            
            scheduler.trigger("correlation-job")
            advanceTimeBy(1000) // Scheduled at now (T=2.5) and picked up at T=3
            
            assertEquals(3, capturedIds.size)
            assertEquals(triggerCorrelationId, capturedIds[2], "Triggered execution should have the trigger correlationId")

        } finally {
            MDC.remove("correlationId")
            scheduler.stop()
        }
    }

    @Test
    fun `cron trigger should calculate next execution time correctly`() {
        val trigger = CronTrigger("0 * * * *") // Every hour on the hour
        val now = Instant.parse("2026-04-25T10:15:00Z")
        val next = trigger.nextExecutionTime(now)
        assertEquals(Instant.parse("2026-04-25T11:00:00Z"), next)
    }

    @Test
    fun `scheduler config should validate retention policy`() {
        val store = MockJobStore()
        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                retention {
                    maxAge = (-1).seconds
                }
            }
            Scheduler(config)
        }

        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                retention {
                    cleanupInterval = 0.seconds
                }
            }
            Scheduler(config)
        }

        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                retention {
                    // Test Java duration overload
                    maxAge(java.time.Duration.ofSeconds(-5))
                }
            }
            Scheduler(config)
        }

        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                retention {
                    batchSize = 0
                }
            }
            Scheduler(config)
        }
    }

    @Test
    fun `scheduler should periodically clean up completed executions according to retention policy`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            pollingInterval = Duration.ofMillis(500)
            retention {
                enabled = true
                maxAge = 10.minutes
                cleanupInterval = 1.minutes
                batchSize = 10
            }
        }

        val scheduler = Scheduler(config, backgroundScope, clock)

        // T = 0
        val oldSuccess = JobExecution(jobId = "old-succ", scheduledAt = Instant.now(clock))
        val oldDead = JobExecution(jobId = "old-dead", scheduledAt = Instant.now(clock))
        store.saveExecution(oldSuccess)
        store.saveExecution(oldDead)
        store.updateExecutionStatus(oldSuccess.id, ExecutionStatus.SUCCESS)
        store.updateExecutionStatus(oldDead.id, ExecutionStatus.DEAD_LETTERED)

        scheduler.start()

        // Advance 11 minutes (so executions become older than maxAge of 10m)
        advanceTimeBy(11 * 60 * 1000L)

        // A new recent success at T = 11m
        val recentSuccess = JobExecution(jobId = "recent-succ", scheduledAt = Instant.now(clock))
        store.saveExecution(recentSuccess)
        store.updateExecutionStatus(recentSuccess.id, ExecutionStatus.SUCCESS)

        // Advance 1 more minute so cleanup interval triggers again
        advanceTimeBy(60 * 1000L)

        // Old success should have been cleaned up
        assertNull(store.getExecution(oldSuccess.id), "Old success should be cleaned up")
        // Dead lettered execution should be preserved by default
        assertNotNull(store.getExecution(oldDead.id), "Dead lettered execution should be preserved")
        // Recent success should be preserved
        assertNotNull(store.getExecution(recentSuccess.id), "Recent success should be preserved")

        scheduler.stop()
    }

    @Test
    fun `job builder should support kotlin time duration for timeout`() {
        val builder = JobBuilder("timeout-job").apply {
            every(1.minutes)
            timeout(5.minutes)
            execute {}
        }
        val jobDef = builder.build()
        assertEquals(Duration.ofMinutes(5), jobDef.timeout)
    }

    @Test
    fun `retry policy builder should support kotlin time duration`() {
        val builder = RetryPolicyBuilder().apply {
            initialDelay(2.seconds)
            maxDelay(10.minutes)
        }
        val retryPolicy = builder.build()
        assertEquals(Duration.ofSeconds(2), retryPolicy.initialDelay)
        assertEquals(Duration.ofMinutes(10), retryPolicy.maxDelay)
    }

    @Test
    fun `interval trigger should support kotlin time duration constructor`() {
        val trigger = IntervalTrigger(5.minutes, 10.seconds)
        assertEquals(Duration.ofMinutes(5), trigger.interval)
        assertEquals(Duration.ofSeconds(10), trigger.initialDelay)
    }

    @Test
    fun `stale worker finishing after reclamation must not overwrite status or schedule next run`() = runBlocking {
        val store = MockJobStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val handlerStarted = CompletableDeferred<Unit>()
        val allowHandlerFinish = CompletableDeferred<Unit>()
        val jobDef = JobBuilder("stale-finish-job").apply {
            every(Duration.ofMinutes(5))
            execute {
                handlerStarted.complete(Unit)
                withContext(NonCancellable) {
                    allowHandlerFinish.await()
                }
            }
        }.build()
        val execution = JobExecution(
            jobId = jobDef.id,
            scheduledAt = Instant.now().minusSeconds(5),
            lockKey = jobDef.lockKey
        )
        store.saveJob(jobDef)
        store.saveExecution(execution)

        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                this.executionLeaseDuration = Duration.ofMinutes(1)
                this.heartbeatInterval = Duration.ofSeconds(30)
                jobs.add(jobDef)
            },
            scope
        )

        scheduler.pollAndExecute()
        handlerStarted.await()

        // Simulate lease expiry and another worker reclaiming the execution while handler is still running
        store.executions.computeIfPresent(execution.id) { _, exec ->
            exec.copy(expiresAt = Instant.now().minusSeconds(1))
        }
        assertTrue(store.claimExecution(execution.id, "other-worker:claim-2", Duration.ofMinutes(5)))

        allowHandlerFinish.complete(Unit)
        delay(50)
        scheduler.stop()
        scope.cancel()

        val current = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CLAIMED, current?.status, "Reclaimed execution status must not be overwritten by stale worker")
        assertEquals("other-worker:claim-2", current?.workerId)
        assertEquals(1, store.executions.size, "Stale worker must not schedule a duplicate next recurring execution")
    }

    @Test
    fun `same scheduler does not reclaim an execution that is still locally active`() = runBlocking {
        val store = MockJobStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val firstAttemptStarted = CompletableDeferred<Unit>()
        val releaseFirstAttempt = CompletableDeferred<Unit>()
        var attempts = 0
        val jobDef = JobBuilder("local-active-job").apply {
            once()
            execute {
                attempts++
                firstAttemptStarted.complete(Unit)
                withContext(NonCancellable) {
                    releaseFirstAttempt.await()
                }
            }
        }.build()
        val execution = JobExecution(
            jobId = jobDef.id,
            scheduledAt = Instant.now().minusSeconds(5)
        )
        store.saveJob(jobDef)
        store.saveExecution(execution)

        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                this.executionLeaseDuration = Duration.ofMinutes(1)
                this.heartbeatInterval = Duration.ofSeconds(30)
                jobs.add(jobDef)
            },
            scope
        )

        scheduler.pollAndExecute()
        firstAttemptStarted.await()

        // Force lease expiry in store while coroutine is still active locally
        store.executions.computeIfPresent(execution.id) { _, exec ->
            exec.copy(expiresAt = Instant.now().minusSeconds(1))
        }

        // Second poll on the same scheduler must skip the locally active execution
        scheduler.pollAndExecute()
        delay(30)
        assertEquals(1, attempts, "Same scheduler must not launch a duplicate local coroutine for an already active execution")

        releaseFirstAttempt.complete(Unit)
        delay(50)
        scheduler.stop()
        scope.cancel()
    }

    @Test
    fun `shutdown while post-claim lock check is in flight releases the claimed execution back to PENDING`() = runBlocking {
        val lockCheckEntered = CompletableDeferred<Unit>()
        val allowLockCheckExit = CompletableDeferred<Unit>()
        val store = object : MockJobStore() {
            override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean {
                if (excludeExecutionId != null) {
                    lockCheckEntered.complete(Unit)
                    allowLockCheckExit.await()
                }
                return super.isLockHeld(lockKey, excludeExecutionId)
            }
        }
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val jobDef = JobBuilder("shutdown-mid-claim-job").apply {
            once()
            concurrencyPolicy = ConcurrencyPolicy.FORBID
            lockKey = "mid-claim-lock"
            execute {
                delay(5_000)
            }
        }.build()
        val execution = JobExecution(
            jobId = jobDef.id,
            scheduledAt = Instant.now().minusSeconds(5),
            lockKey = jobDef.lockKey
        )
        store.saveJob(jobDef)
        store.saveExecution(execution)

        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                this.pollingInterval = Duration.ofMillis(50)
                this.shutdownTimeout = Duration.ofMillis(50)
                jobs.add(jobDef)
            },
            scope
        )

        scheduler.start()
        lockCheckEntered.await()

        val stopJob = scope.launch {
            scheduler.stop()
        }
        delay(20)
        allowLockCheckExit.complete(Unit)
        stopJob.join()
        scope.cancel()

        val afterStop = store.getExecution(execution.id)
        assertEquals(
            ExecutionStatus.PENDING,
            afterStop?.status,
            "An execution claimed right before scheduler.stop() must be released back to PENDING"
        )
        assertNull(afterStop?.workerId)
    }
}

