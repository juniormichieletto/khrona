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
import java.util.concurrent.atomic.AtomicInteger

class BoundedPollingTest {

    class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    @Test
    fun `scheduler should only poll up to pollBatchSize executions`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val executedCount = AtomicInteger(0)
        
        val config = KhronaConfig().apply {
            this.store = store
            pollBatchSize = 5
            pollingInterval = Duration.ofSeconds(10) // Long enough to only poll once in advanceTimeBy(1100)
            job("batch-job") {
                every(Duration.ofHours(1))
                execute { executedCount.incrementAndGet() }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        val jobDef = config.jobs.first()
        store.saveJob(jobDef)
        
        // Create 10 eligible executions
        repeat(10) { i ->
            store.saveExecution(JobExecution(
                jobId = jobDef.id, 
                scheduledAt = Instant.now(clock).minusSeconds(10 + i.toLong())
            ))
        }

        scheduler.start()
        
        // Wait for first poll
        advanceTimeBy(1100)
        
        // Should only have executed 5 jobs
        assertEquals(5, executedCount.get(), "Should have only executed one batch of 5")
        
        // Wait for next poll
        advanceTimeBy(config.pollingInterval.toMillis())
        
        // Now should have executed all 10
        assertEquals(10, executedCount.get(), "Should have executed second batch after next poll")
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should fail to start with invalid pollBatchSize`() = runTest {
        val store = MockJobStore()
        
        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                pollBatchSize = 0
            }
            Scheduler(config, this)
        }

        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                pollBatchSize = -1
            }
            Scheduler(config, this)
        }
    }
}
