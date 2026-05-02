package io.khrona.core

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

@OptIn(ExperimentalCoroutinesApi::class)
class RecurringResilienceTest {

    @Test
    fun `recurring job should continue after terminal failure`() = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val callCount = AtomicInteger(0)
        
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            job("resilient-recurring") {
                every(Duration.ofSeconds(1))
                retry {
                    maxAttempts = 1 // No retries, fails immediately to DEAD_LETTERED
                }
                execute {
                    callCount.incrementAndGet()
                    throw RuntimeException("Permanent failure")
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.start()
        
        // 1. Wait for first execution (T=1s)
        advanceTimeBy(1500)
        assertEquals(1, callCount.get())
        
        val firstExec = store.executions.values.find { it.jobId == "resilient-recurring" && it.attempt == 0 }
        assertNotNull(firstExec)
        assertEquals(ExecutionStatus.DEAD_LETTERED, firstExec?.status)
        
        // 2. In the buggy state, the next occurrence (T=2s) will NOT be scheduled.
        // Let's check if it exists.
        val nextExec = store.executions.values.find { 
            it.jobId == "resilient-recurring" && it.status == ExecutionStatus.PENDING 
        }
        
        // This is expected to FAIL before the fix
        assertNotNull(nextExec, "Next occurrence should be scheduled even after DEAD_LETTERED")
        
        // 3. If we advance time, the second one should run
        advanceTimeBy(1000)
        assertEquals(2, callCount.get(), "Second occurrence should have executed")
        
        scheduler.stop()
    }
}
