@file:OptIn(ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class SchedulerTest {

    class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    @Test
    fun `scheduler should execute eligible jobs`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            job("test-job") {
                every(Duration.ofMinutes(1))
                execute {
                    // Job logic
                }
            }
        }
        
        val clock = TestClock(testScheduler)
        val scheduler = Scheduler(config, this, clock)
        
        // Setup an eligible execution
        val execution = JobExecution(jobId = "test-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        
        // Wait for polling and execution
        advanceTimeBy(2000)
        
        // Check if status reached SUCCESS
        // We might need a bit of real delay or more advanceTime if things are launched in other scopes
        // But since we pass 'this' as scope, it should be controlled by runTest
        
        yield() // Allow other coroutines to run
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.SUCCESS, updated?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should handle job failure`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            job("fail-job") {
                every(Duration.ofMinutes(1))
                execute {
                    throw RuntimeException("Boom")
                }
            }
        }
        
        val clock = TestClock(testScheduler)
        val scheduler = Scheduler(config, this, clock)
        
        val execution = JobExecution(jobId = "fail-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        advanceTimeBy(2000)
        yield()
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.FAILED, updated?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should retry failed jobs`() = runTest {
        val store = MockJobStore()
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
        
        val clock = TestClock(testScheduler)
        val scheduler = Scheduler(config, this, clock)
        
        val execution = JobExecution(jobId = "retry-job", scheduledAt = Instant.now(clock).minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        
        // First execution fails
        advanceTimeBy(1000)
        yield()
        
        // Should have a new execution with attempt 1
        val retryExec = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 1 }
        assertNotNull(retryExec, "Retry execution should be scheduled")
        // It might be already CLAIMED if the scheduler loop ran again
        
        // Wait for retry to be eligible and executed
        advanceTimeBy(2000)
        yield()
        
        // Should have attempt 2
        val retryExec2 = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 2 }
        assertNotNull(retryExec2, "Second retry execution should be scheduled")
        
        // Wait for second retry
        advanceTimeBy(2000)
        yield()
        
        // Should be DEAD_LETTERED now as maxAttempts = 3 (0, 1, 2 are 3 attempts)
        val finalExec = store.executions.values.find { it.jobId == "retry-job" && it.attempt == 2 }
        assertEquals(ExecutionStatus.DEAD_LETTERED, finalExec?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should fail to start if job interval is smaller than polling interval`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofSeconds(5)
            job("invalid-job") {
                every(Duration.ofSeconds(1)) // 1s < 5s
                execute {}
            }
        }
        
        val scheduler = Scheduler(config, this)
        assertThrows(IllegalArgumentException::class.java) {
            scheduler.start()
        }
    }

    @Test
    fun `scheduler should start if job interval is greater than or equal to polling interval`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(10)
            job("valid-job") {
                every(Duration.ofMillis(100))
                execute {}
            }
        }
        
        val scheduler = Scheduler(config, this)
        assertDoesNotThrow {
            scheduler.start()
        }
        scheduler.stop()
    }

    @Test
    fun `registerJob should fail if job interval is smaller than polling interval`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofSeconds(5)
        }
        
        val scheduler = Scheduler(config, this)
        val jobDef = JobBuilder("invalid-job").apply {
            every(Duration.ofSeconds(1))
            execute {}
        }.build()

        assertThrows(IllegalArgumentException::class.java) {
            scheduler.registerJob(jobDef)
        }
    }
}
