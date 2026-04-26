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
        
        val scheduler = Scheduler(config, this, clock)
        assertThrows(IllegalArgumentException::class.java) {
            scheduler.start()
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
        
        val scheduler = Scheduler(config, this, clock)
        assertThrows(IllegalArgumentException::class.java) {
            scheduler.start()
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
            scheduler.registerJob(jobDef)
        }
    }

    @Test
    fun `should not allow multiple triggers in DSL`() {
        assertThrows(IllegalStateException::class.java) {
            KhronaConfig().job("multi-trigger") {
                every(Duration.ofMinutes(1))
                cron("0 * * * *")
                execute {}
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
        
        // Manually trigger
        scheduler.trigger("manual-job")
        
        advanceTimeBy(5000)
        
        assertEquals(1, callCount, "Job should have been executed once due to manual trigger")
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should propagate correlationId`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        var capturedCorrelationId: String? = null
        
        val config = KhronaConfig().apply {
            this.store = store
            job("correlation-job") {
                once()
                execute {
                    capturedCorrelationId = MDC.get("correlationId")
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        
        // Manually set a correlationId in MDC before triggering
        val originalCorrelationId = "test-correlation-id"
        MDC.put("correlationId", originalCorrelationId)
        try {
            scheduler.start()
            
            // Wait for polling and execution
            advanceTimeBy(5000)
            
            assertEquals(originalCorrelationId, capturedCorrelationId, "CorrelationId should be propagated from registration context")
            
            // Test trigger propagation
            capturedCorrelationId = null
            val triggerCorrelationId = "trigger-correlation-id"
            MDC.put("correlationId", triggerCorrelationId)
            
            scheduler.trigger("correlation-job")
            advanceTimeBy(5000)
            
            assertEquals(triggerCorrelationId, capturedCorrelationId, "CorrelationId should be propagated from trigger context")
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
}
