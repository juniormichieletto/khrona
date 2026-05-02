package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

class SchedulerDistributedTest {

    @Test
    fun `should prevent concurrent execution of jobs with FORBID policy`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(10)
        }
        
        val counter = AtomicInteger(0)
        val jobDef = JobDefinition(
            id = "forbidden-job",
            handler = {
                counter.incrementAndGet()
                delay(1000) // Simulate long running job
            },
            trigger = IntervalTrigger(Duration.ofMillis(100)),
            concurrencyPolicy = ConcurrencyPolicy.FORBID,
            lockKey = "my-lock"
        )
        
        store.saveJob(jobDef)
        
        val scheduler1 = Scheduler(config, this)
        val scheduler2 = Scheduler(config, this)
        
        // Register the job in both schedulers to populate their HandlerRegistries
        scheduler1.registerJob(jobDef)
        scheduler2.registerJob(jobDef)
        
        // Manually trigger an execution
        store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = Instant.now(), lockKey = "my-lock"))
        
        // Start both schedulers
        val job1 = launch { scheduler1.pollAndExecute() }
        val job2 = launch { scheduler2.pollAndExecute() }
        
        job1.join()
        job2.join()
        
        assertEquals(1, counter.get(), "Only one scheduler should have started the job")
    }
}
