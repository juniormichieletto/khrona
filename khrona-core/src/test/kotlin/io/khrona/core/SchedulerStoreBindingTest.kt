@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.*
import java.util.UUID

class SchedulerStoreBindingTest {
    class TestClock(private val time: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(time.currentTime)
    }
    @Test
    fun `store replacement must not redirect completion of an already running execution`() = runTest {
        val clock = TestClock(testScheduler)
        val original = MockJobStore(clock)
        val replacement = MockJobStore(clock)
        val finish = CompletableDeferred<Unit>()
        val config = KhronaConfig().apply {
            store = original
            job("busy") { every(Duration.ofHours(1)); execute { finish.await() } }
        }
        config.jobs.forEach { original.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        original.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        assertEquals(ExecutionStatus.RUNNING, original.executions[execution.id]!!.status)
        config.store = replacement
        finish.complete(Unit); runCurrent()
        assertEquals(ExecutionStatus.SUCCESS, original.executions[execution.id]!!.status,"completion must target the store that owns the claim")
    }

    @Test
    fun `heartbeat continues renewing the original store after replacement`() = runTest {
        val clock = TestClock(testScheduler)
        val original = MockJobStore(clock)
        val replacement = MockJobStore(clock)
        var cancelled = false
        val config = KhronaConfig().apply {
            store = original
            executionLeaseDuration = Duration.ofSeconds(2)
            heartbeatInterval = Duration.ofMillis(500)
            job("busy") { every(Duration.ofHours(1)); execute { try { awaitCancellation() } finally { cancelled = true } } }
        }
        config.jobs.forEach { original.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        original.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        config.store = replacement
        advanceTimeBy(1500); runCurrent()
        assertFalse(cancelled, "replacement must not invalidate the original claim")
        assertTrue(original.executions[execution.id]!!.expiresAt!! > Instant.EPOCH.plusSeconds(2))
    }

    @Test
    fun `retry is persisted in original store after replacement`() = runTest {
        val clock = TestClock(testScheduler)
        val original = MockJobStore(clock)
        val replacement = MockJobStore(clock)
        val finish = CompletableDeferred<Unit>()
        val config = KhronaConfig().apply {
            store = original
            job("busy") { every(Duration.ofHours(1)); execute { finish.await(); error("handler failure") } }
        }
        config.jobs.forEach { original.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        original.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        config.store = replacement
        finish.complete(Unit); runCurrent()
        assertEquals(ExecutionStatus.FAILED, original.executions[execution.id]!!.status)
        assertEquals(1, original.executions.values.count { it.attempt == 1 && it.status == ExecutionStatus.PENDING })
        assertTrue(replacement.executions.isEmpty())
    }

    @Test
    fun `shutdown releases the claim in its original store after replacement`() = runBlocking {
        val original = MockJobStore()
        val replacement = MockJobStore()
        val entered = CompletableDeferred<Unit>()
        val workerScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val config = KhronaConfig().apply {
            store = original
            shutdownTimeout = Duration.ofMillis(100)
            job("busy") { every(Duration.ofHours(1)); execute { entered.complete(Unit); awaitCancellation() } }
        }
        config.jobs.forEach { original.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        original.saveExecution(execution)
        val scheduler = Scheduler(config, workerScope)
        try {
            scheduler.pollAndExecute()
            withTimeout(2000) { entered.await() }
            config.store = replacement
            scheduler.stop()
            withTimeout(2000) {
                while (original.executions[execution.id]!!.status != ExecutionStatus.PENDING) delay(5)
            }
            assertTrue(replacement.executions.isEmpty())
        } finally { workerScope.cancel() }
    }
}
