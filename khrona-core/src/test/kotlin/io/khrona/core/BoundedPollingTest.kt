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

        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                maxConcurrentExecutions = 0
            }
            Scheduler(config, this)
        }
    }

    @Test
    fun `older pending executions without a local handler do not starve local jobs when pollBatchSize is reached`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val now = Instant.now(clock)
        val myRuns = AtomicInteger(0)

        store.saveJob(JobDefinition(id = "foreign-job", trigger = OneTimeTrigger(now.minusSeconds(100)), handler = {}))
        repeat(5) { idx ->
            store.saveExecution(
                JobExecution(
                    jobId = "foreign-job",
                    scheduledAt = now.minusSeconds(100L - idx)
                )
            )
        }

        val myJob = JobDefinition(
            id = "mine",
            trigger = OneTimeTrigger(now.minusSeconds(10)),
            handler = { myRuns.incrementAndGet() }
        )
        store.saveJob(myJob)
        val myExecution = JobExecution(jobId = "mine", scheduledAt = now.minusSeconds(10))
        store.saveExecution(myExecution)

        val config = KhronaConfig().apply {
            this.store = store
            this.pollBatchSize = 5
            jobs.add(myJob)
        }
        val scheduler = Scheduler(config, backgroundScope, clock)

        scheduler.pollAndExecute()
        advanceUntilIdle()
        scheduler.stop()

        assertEquals(1, myRuns.get(), "Local job must execute even when pollBatchSize older foreign rows exist")
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(myExecution.id)?.status)
    }

    @Test
    fun `scheduler bounds concurrent active executions across polls`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val active = AtomicInteger(0)
        val maxActive = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            pollBatchSize = 100
            maxConcurrentExecutions = 10
            job("allow-burst") {
                once()
                concurrencyPolicy = ConcurrencyPolicy.ALLOW
                execute {
                    val current = active.incrementAndGet()
                    maxActive.accumulateAndGet(current, ::maxOf)
                    delay(5_000)
                    active.decrementAndGet()
                }
            }
        }

        val scheduler = Scheduler(config, backgroundScope, clock)
        val jobDef = config.jobs.first()
        store.saveJob(jobDef)
        repeat(1_000) {
            store.saveExecution(JobExecution(jobId = jobDef.id, scheduledAt = Instant.now(clock).minusSeconds(1)))
        }

        repeat(10) {
            scheduler.pollAndExecute()
            advanceTimeBy(20)
        }

        assertEquals(
            10,
            maxActive.get(),
            "Concurrent active handlers (${maxActive.get()}) must not exceed maxConcurrentExecutions (10)"
        )
        scheduler.stop()
    }

    @Test
    fun `bad job definition does not abort healthy execution batch`() = runTest {
        val clock = TestClock(testScheduler)
        val store = object : MockJobStore(clock) {
            override suspend fun getJob(jobId: String): JobDefinition? {
                if (jobId == "bad") error("unsupported trigger in stored definition")
                return super.getJob(jobId)
            }
        }
        var healthyRuns = 0
        val config = KhronaConfig().apply {
            this.store = store
            job("bad") { every(Duration.ofHours(1)); execute {} }
            job("healthy") { every(Duration.ofHours(1)); execute { healthyRuns++ } }
        }
        config.jobs.forEach { store.saveJob(it) }
        store.saveExecution(JobExecution(jobId = "bad", scheduledAt = Instant.EPOCH.minusSeconds(1)))
        store.saveExecution(JobExecution(jobId = "healthy", scheduledAt = Instant.EPOCH))
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute()
        runCurrent()
        assertEquals(1, healthyRuns, "Failure loading one job definition must not abort healthy jobs in the batch")
    }
}
