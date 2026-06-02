package io.khrona.core

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

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

    @Test
    fun `should work with CronTrigger misfire`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock) // Test clock usually starts at a fixed epoch
        
        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-cron") {
                cron("0 * * * *") // Every hour
                misfirePolicy = MisfirePolicy.IGNORE
                execute {}
            }
        }

        val scheduler = Scheduler(config, this, clock)
        
        // Misfire an hour ago
        val misfiredAt = now.minus(Duration.ofHours(1))
        val execution = JobExecution(jobId = "misfire-cron", scheduledAt = misfiredAt)
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()
        
        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(execution.id)?.status)
        
        // Check next scheduled
        val nextExec = store.executions.values.find { it.jobId == "misfire-cron" && it.status == ExecutionStatus.PENDING }
        // CronTrigger "0 * * * *" should schedule the next hour after 'now'
        val trigger = CronTrigger("0 * * * *")
        val expectedNext = trigger.nextExecutionTime(now)
        assertEquals(expectedNext, nextExec?.scheduledAt)
    }

    @Test
    fun `cron misfire should fire now by default`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock)

        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-cron-default") {
                cron("0 * * * *")
                execute {}
            }
        }

        val scheduler = Scheduler(config, this, clock)

        val execution = JobExecution(
            jobId = "misfire-cron-default",
            scheduledAt = now.minus(Duration.ofHours(1))
        )
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `cron misfire should fire now when policy is explicitly FIRE_NOW`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock)

        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-cron-fire-now") {
                cron("0 * * * *")
                misfirePolicy = MisfirePolicy.FIRE_NOW
                execute {}
            }
        }

        val scheduler = Scheduler(config, this, clock)

        val execution = JobExecution(
            jobId = "misfire-cron-fire-now",
            scheduledAt = now.minus(Duration.ofHours(1))
        )
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `timezone cron misfire should ignore and schedule next run in configured timezone`() = runTest {
        val store = MockJobStore()
        val clock = SchedulerTest.TestClock(testScheduler)
        val now = Instant.now(clock)

        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("misfire-cron-new-york") {
                cron("0 9 * * *", "America/New_York")
                misfirePolicy = MisfirePolicy.IGNORE
                execute {}
            }
        }

        val scheduler = Scheduler(config, this, clock)

        val execution = JobExecution(
            jobId = "misfire-cron-new-york",
            scheduledAt = now.minus(Duration.ofHours(1))
        )
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(execution.id)?.status)

        val nextExec = store.executions.values.find {
            it.jobId == "misfire-cron-new-york" && it.status == ExecutionStatus.PENDING
        }
        val expectedNext = CronTrigger("0 9 * * *", timeZone = "America/New_York").nextExecutionTime(now)
        assertEquals(expectedNext, nextExec?.scheduledAt)
    }

    @Test
    fun `concurrent schedulers should persist one next execution when ignoring the same misfire`() = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val bothSchedulersFetchedExecution = CompletableDeferred<Unit>()
        val eligibleFetchCount = AtomicInteger()
        val store = object : MockJobStore(clock) {
            override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> {
                val eligible = super.listEligibleExecutions(now, limit)
                if (eligibleFetchCount.incrementAndGet() == 2) {
                    bothSchedulersFetchedExecution.complete(Unit)
                }
                bothSchedulersFetchedExecution.await()
                return eligible
            }
        }
        val now = Instant.now(clock)
        val config = KhronaConfig().apply {
            this.store = store
            this.misfireThreshold = Duration.ofSeconds(30)
            job("concurrent-misfire-ignore") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                execute {}
            }
        }
        val firstScheduler = Scheduler(config, this, clock)
        val secondScheduler = Scheduler(config, this, clock)
        val execution = JobExecution(
            jobId = "concurrent-misfire-ignore",
            scheduledAt = now.minus(Duration.ofMinutes(10))
        )
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        val firstPoll = launch { firstScheduler.pollAndExecute() }
        val secondPoll = launch { secondScheduler.pollAndExecute() }
        firstPoll.join()
        secondPoll.join()

        val pendingExecutions = store.executions.values.filter {
            it.jobId == execution.jobId && it.status == ExecutionStatus.PENDING
        }
        assertEquals(1, pendingExecutions.size)
        val expectedNext = now.plus(Duration.ofMinutes(1))
        val expectedId = UUID.nameUUIDFromBytes("${execution.jobId}:$expectedNext".toByteArray())
        assertEquals(expectedId, pendingExecutions.single().id)
    }
}
