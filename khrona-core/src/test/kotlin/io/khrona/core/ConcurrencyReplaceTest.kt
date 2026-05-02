package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

@OptIn(ExperimentalCoroutinesApi::class)
class ConcurrencyReplaceTest {

    @Test
    fun `REPLACE policy should cancel running job and start new one`() = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val startCount = AtomicInteger(0)
        val completionCount = AtomicInteger(0)
        
        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            job("replaced-job") {
                at(Instant.now(clock).plus(Duration.ofDays(365)))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = "shared-lock"
                execute {
                    val current = startCount.incrementAndGet()
                    println("Job started (count: $current, execution: ${coroutineContext[Job]})")
                    try {
                        delay(2000) // Long job
                        completionCount.incrementAndGet()
                        println("Job completed (count: $current)")
                    } catch (e: CancellationException) {
                        println("Job cancelled (count: $current)")
                        throw e
                    }
                }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.start()
        
        // 1. Trigger first execution
        println("Saving execution 1")
        store.saveExecution(JobExecution(id = UUID.nameUUIDFromBytes("exec1".toByteArray()), jobId = "replaced-job", scheduledAt = Instant.now(clock), lockKey = "shared-lock"))
        advanceTimeBy(150) // Let it start
        assertEquals(1, startCount.get(), "First job should have started")
        assertEquals(0, completionCount.get(), "First job should not have completed")
        
        // 2. Trigger second execution while first is running
        println("Saving execution 2")
        store.saveExecution(JobExecution(id = UUID.nameUUIDFromBytes("exec2".toByteArray()), jobId = "replaced-job", scheduledAt = Instant.now(clock), lockKey = "shared-lock"))
        advanceTimeBy(200) // Polling interval is 100ms
        
        // 3. Verification
        // First job should have been cancelled, second should have started
        assertEquals(2, startCount.get(), "Only two jobs should have started (one original, one replacement)")
        assertEquals(0, completionCount.get(), "First job should NOT have finished")
        
        // Check store status
        val executions = store.executions.values.filter { it.jobId == "replaced-job" }.sortedBy { it.startedAt }
        val superseded = executions.find { it.status == ExecutionStatus.SUPERSEDED }
        assertNotNull(superseded, "One execution should be SUPERSEDED")
        
        scheduler.stop()
    }

    @Test
    fun `REPLACE policy should cancel job running on another node via heartbeat failure`() = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val node1Started = AtomicInteger(0)
        val node1Cancelled = AtomicInteger(0)
        
        val config1 = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            job("dist-job") {
                at(Instant.now(clock).plus(Duration.ofDays(365)))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = "dist-lock"
                execute {
                    node1Started.incrementAndGet()
                    try {
                        delay(5000) // Long job
                    } catch (e: CancellationException) {
                        node1Cancelled.incrementAndGet()
                        throw e
                    }
                }
            }
        }

        val config2 = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            job("dist-job") {
                at(Instant.now(clock).plus(Duration.ofDays(365)))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = "dist-lock"
                execute {
                    // Node 2 handler
                }
            }
        }
        
        val scheduler1 = Scheduler(config1, backgroundScope, clock)
        val scheduler2 = Scheduler(config2, backgroundScope, clock)
        
        scheduler1.start()
        scheduler2.start()
        
        // 1. Node 1 starts the job
        store.saveExecution(JobExecution(id = UUID.randomUUID(), jobId = "dist-job", scheduledAt = Instant.now(clock), lockKey = "dist-lock"))
        advanceTimeBy(150)
        assertEquals(1, node1Started.get())
        
        // 2. Node 2 triggers replacement
        // Note: In a real app, Node 2 would see a NEW execution and supersede the old one
        store.saveExecution(JobExecution(id = UUID.randomUUID(), jobId = "dist-job", scheduledAt = Instant.now(clock), lockKey = "dist-lock"))
        advanceTimeBy(200) 
        
        // 3. Verification: Node 1 should eventually stop because its heartbeat will fail
        // Heartbeat happens every leaseDuration / 2. Default lease is 5m? No, I should check.
        // In Scheduler.kt it's currently hardcoded to 5m.
        // Let's check if I can wait for it.
        // Wait, virtual time means we can advance quickly.
        advanceTimeBy(Duration.ofMinutes(10).toMillis())
        
        assertEquals(1, node1Cancelled.get(), "Node 1 should have been cancelled via heartbeat failure")
        
        scheduler1.stop()
        scheduler2.stop()
    }
}
