package io.khrona.core

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

@OptIn(ExperimentalCoroutinesApi::class)
class TimeoutTest {

    @Test
    fun `should enforce job timeout`() = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val wasInterrupted = AtomicBoolean(false)
        
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            job("timeout-job") {
                once()
                timeout = Duration.ofMillis(500)
                retry {
                    maxAttempts = 1 // Fail immediately to DEAD_LETTERED
                }
                execute {
                    try {
                        delay(2000) // Longer than timeout
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        wasInterrupted.set(true)
                        throw e
                    }
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.start()
        
        // Wait for execution and timeout
        advanceTimeBy(3000)
        
        // Find the specific execution that should be DEAD_LETTERED
        val execution = store.executions.values.find { it.jobId == "timeout-job" && it.attempt == 0 }
        assertNotNull(execution)
        assertEquals(ExecutionStatus.DEAD_LETTERED, execution?.status, "Job should be DEAD_LETTERED due to timeout")
        assertTrue(wasInterrupted.get(), "Job handler should have been interrupted by timeout")
        assertTrue(execution?.error?.lowercase()?.contains("timeout") == true, "Error message should mention timeout")
        
        scheduler.stop()
    }
}
