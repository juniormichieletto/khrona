package io.khrona.core

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

@OptIn(ExperimentalCoroutinesApi::class)
class MisfireTest {

    @Test
    fun `should ignore misfired execution when policy is IGNORE`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock)
        
        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-ignore") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                execute {
                    // logic
                }
            }
        }

        val scheduler = Scheduler(config, this, clock)
        
        // Create an execution far in the past (beyond 30s threshold)
        val misfiredAt = now.minus(Duration.ofMinutes(10))
        val execution = JobExecution(jobId = "misfire-ignore", scheduledAt = misfiredAt)
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()
        
        // Execution should be marked as MISFIRED
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.MISFIRED, updated?.status)
        
        // Should have scheduled the next execution
        val nextExec = store.executions.values.find { it.jobId == "misfire-ignore" && it.status == ExecutionStatus.PENDING }
        assertEquals(now.plus(Duration.ofMinutes(1)), nextExec?.scheduledAt)
    }

    @Test
    fun `should fire misfired execution when policy is FIRE_NOW`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock)
        
        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-fire-now") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.FIRE_NOW
                execute {
                    // logic
                }
            }
        }

        val scheduler = Scheduler(config, this, clock)
        
        // Create an execution in the past
        val misfiredAt = now.minus(Duration.ofMinutes(10))
        val execution = JobExecution(jobId = "misfire-fire-now", scheduledAt = misfiredAt)
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()
        
        // It should NOT be MISFIRED, it should be CLAIMED or RUNNING (or SUCCESS if we waited)
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CLAIMED, updated?.status)
    }
}
