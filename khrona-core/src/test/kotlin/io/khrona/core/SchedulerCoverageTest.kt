package io.khrona.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.MDC
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class SchedulerCoverageTest {
    @Test
    fun `scheduler validates required store and positive timing configuration`() {
        assertThrows(IllegalStateException::class.java) {
            Scheduler(KhronaConfig())
        }
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config { executionLeaseDuration = Duration.ZERO })
        }
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config {
                executionLeaseDuration = Duration.ofSeconds(10)
                heartbeatInterval = Duration.ZERO
            })
        }
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config {
                executionLeaseDuration = Duration.ofSeconds(10)
                heartbeatInterval = Duration.ofSeconds(10)
            })
        }
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config { pollBatchSize = 0 })
        }
    }

    @Test
    fun `scheduler rejects negative execution lease duration`() {
        val error = assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config { executionLeaseDuration = Duration.ofMillis(-1) })
        }

        assertEquals("executionLeaseDuration must be positive", error.message)
    }

    @Test
    fun `scheduler validates interval and cron frequencies against polling interval`() {
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config {
                pollingInterval = Duration.ofSeconds(2)
                jobs.add(job("fast-interval", IntervalTrigger(Duration.ofSeconds(1))))
            })
        }
        assertThrows(IllegalArgumentException::class.java) {
            Scheduler(config {
                pollingInterval = Duration.ofMinutes(2)
                jobs.add(job("fast-cron", CronTrigger("* * * * *")))
            })
        }

        val store = MockJobStore()
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job("impossible-cron", CronTrigger("0 0 30 2 *")))
        })
        org.junit.jupiter.api.Assertions.assertNotNull(scheduler)
    }

    @Test
    fun `start is idempotent and trigger rejects missing jobs`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val store = MockJobStore()
        val scheduler = Scheduler(config { this.store = store }, scope)

        scheduler.start()
        scheduler.start()

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                scheduler.trigger("missing")
            }
        }

        scheduler.stop()
        scope.cancel()
    }

    @Test
    fun `registerJob schedules one time job using epoch and does not duplicate deterministic execution`() = runBlocking {
        val clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00Z"), ZoneOffset.UTC)
        val store = MockJobStore(clock)
        val scheduler = Scheduler(config { this.store = store }, clock = clock)
        val job = job("one-time-register", OneTimeTrigger(Instant.EPOCH))

        scheduler.registerJob(job)
        scheduler.registerJob(job)

        assertEquals(1, store.executions.size)
        assertEquals(Instant.EPOCH, store.executions.values.single().scheduledAt)
    }

    @Test
    fun `scheduler loop catches store polling errors`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler = Scheduler(
            config { store = ThrowingPollStore() },
            scope
        )

        scheduler.start()
        delay(50)
        scheduler.stop()
        scope.cancel()
    }

    @Test
    fun `scheduler loop logs recovered executions and tolerates jobs without initial schedule`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val store = RecoveringStore()
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job("never-scheduled", OneTimeTrigger(Instant.EPOCH.minusSeconds(1))))
            },
            scope
        )

        scheduler.start()
        delay(50)
        scheduler.stop()
        scope.cancel()

        assertEquals(1, store.recoveryCalls)
        assertEquals(0, store.executions.size)
    }

    @Test
    fun `start does not duplicate existing deterministic initial execution`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00Z"), ZoneOffset.UTC)
        val store = MockJobStore(clock)
        val job = job("initial-existing", IntervalTrigger(Duration.ofMinutes(1)))
        val next = job.trigger.nextExecutionTime(Instant.now(clock))!!
            .truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val deterministicId = UUID.nameUUIDFromBytes("${job.id}:$next".toByteArray())
        store.saveExecution(JobExecution(id = deterministicId, jobId = job.id, scheduledAt = next))
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
            },
            scope,
            clock
        )

        scheduler.start()
        delay(50)
        scheduler.stop()
        scope.cancel()

        assertEquals(1, store.executions.size)
    }

    @Test
    fun `poll skips eligible execution when job definition disappears`() = runBlocking {
        val store = MissingJobStore("missing-definition")
        val execution = JobExecution(jobId = "missing-definition", scheduledAt = Instant.now())
        val job = job("missing-definition", OneTimeTrigger(Instant.now()))
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job)
        })

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.PENDING, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `poll leaves execution pending when claim fails`() = runBlocking {
        val store = ClaimFalseStore()
        val execution = JobExecution(jobId = "claim-fails", scheduledAt = Instant.now())
        val job = job("claim-fails", OneTimeTrigger(Instant.now()))
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job)
        })

        scheduler.pollAndExecute()

        assertEquals(1, store.claimAttempts)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `poll releases claim when another execution holds forbid lock after claim`() = runBlocking {
        val store = OtherLockAfterClaimStore()
        val execution = JobExecution(
            jobId = "forbid-race",
            scheduledAt = Instant.now(),
            lockKey = "shared-lock"
        )
        val job = job("forbid-race", OneTimeTrigger(Instant.now())).copy(
            concurrencyPolicy = ConcurrencyPolicy.FORBID,
            lockKey = "shared-lock"
        )
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job)
        })

        scheduler.pollAndExecute()

        assertEquals(listOf(execution.id to ExecutionStatus.PENDING), store.updatedStatuses)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `misfire ignore does not duplicate existing deterministic successor`() = runBlocking {
        val clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00Z"), ZoneOffset.UTC)
        val store = MockJobStore(clock)
        val execution = JobExecution(
            jobId = "misfire-existing-next",
            scheduledAt = Instant.parse("2029-12-31T23:00:00Z")
        )
        val job = job("misfire-existing-next", IntervalTrigger(Duration.ofMinutes(1))).copy(
            misfirePolicy = MisfirePolicy.IGNORE
        )
        val next = job.trigger.nextExecutionTime(Instant.now(clock))!!
            .truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val deterministicId = UUID.nameUUIDFromBytes("${job.id}:$next".toByteArray())
        store.saveJob(job)
        store.saveExecution(execution)
        store.saveExecution(JobExecution(id = deterministicId, jobId = job.id, scheduledAt = next))
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                misfireThreshold = Duration.ofMinutes(10)
            },
            clock = clock
        )

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(execution.id)?.status)
        assertEquals(2, store.executions.size)
    }

    @Test
    fun `misfire ignore with one time trigger has no successor to schedule`() = runBlocking {
        val clock = Clock.fixed(Instant.parse("2030-01-01T00:00:00Z"), ZoneOffset.UTC)
        val store = MockJobStore(clock)
        val scheduledAt = Instant.parse("2029-12-31T23:00:00Z")
        val execution = JobExecution(jobId = "misfire-once", scheduledAt = scheduledAt)
        val job = job("misfire-once", OneTimeTrigger(scheduledAt)).copy(
            misfirePolicy = MisfirePolicy.IGNORE
        )
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                misfireThreshold = Duration.ofMinutes(10)
            },
            clock = clock
        )

        scheduler.pollAndExecute()

        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(execution.id)?.status)
        assertEquals(1, store.executions.size)
    }

    @Test
    fun `replace policy without lock key executes without superseding`() = runBlocking {
        val store = SupersedeTrackingStore()
        val execution = JobExecution(jobId = "replace-no-lock", scheduledAt = Instant.now())
        val job = job("replace-no-lock", OneTimeTrigger(Instant.now())).copy(
            concurrencyPolicy = ConcurrencyPolicy.REPLACE,
            lockKey = null
        )
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job)
        })

        scheduler.pollAndExecute()
        delay(50)

        assertEquals(false, store.supersedeCalled)
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `replace policy ignores superseded execution that is not active locally`() = runBlocking {
        val store = UnknownSupersededStore()
        val execution = JobExecution(
            jobId = "replace-remote",
            scheduledAt = Instant.now(),
            lockKey = "remote-lock"
        )
        val job = job("replace-remote", OneTimeTrigger(Instant.now())).copy(
            concurrencyPolicy = ConcurrencyPolicy.REPLACE,
            lockKey = "remote-lock"
        )
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(config {
            this.store = store
            jobs.add(job)
        })

        scheduler.pollAndExecute()
        delay(50)

        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `missing handler after precheck is handled as execution failure`() = runBlocking {
        val store = RemovingHandlerStore("handler-removed")
        val execution = JobExecution(jobId = "handler-removed", scheduledAt = Instant.now())
        val job = job("handler-removed", OneTimeTrigger(Instant.now())).copy(
            retryPolicy = RetryPolicy(maxAttempts = 1)
        )
        store.saveJob(job)
        store.saveExecution(execution)
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
            },
            scope
        )
        store.scheduler = scheduler

        scheduler.pollAndExecute()
        delay(50)
        scheduler.stop()
        scope.cancel()

        assertEquals(ExecutionStatus.DEAD_LETTERED, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `registerJob does not create execution when trigger has no next time`() = runBlocking {
        val store = MockJobStore()
        val scheduler = Scheduler(config { this.store = store })
        val job = job("past-before-epoch", OneTimeTrigger(Instant.EPOCH.minusSeconds(1)))

        scheduler.registerJob(job)

        assertEquals(0, store.executions.size)
    }

    @Test
    fun `registerJob propagates existing correlation id`() = runBlocking {
        val store = MockJobStore()
        val scheduler = Scheduler(config { this.store = store })
        val job = job("register-correlation", IntervalTrigger(Duration.ofMinutes(1)))

        MDC.put("correlationId", "existing-correlation")
        try {
            scheduler.registerJob(job)
        } finally {
            MDC.remove("correlationId")
        }

        assertEquals("existing-correlation", store.executions.values.single().correlationId)
    }


    @Test
    fun `shutdown timeout ignores active executions that are gone or already terminal`() = runBlocking {
        val store = TerminalLookupShutdownStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val missing = JobExecution(jobId = "shutdown-missing", scheduledAt = Instant.now())
        val terminal = JobExecution(jobId = "shutdown-terminal", scheduledAt = Instant.now())
        val missingJob = job("shutdown-missing", OneTimeTrigger(Instant.now())) {
            delay(1_000)
        }
        val terminalJob = job("shutdown-terminal", OneTimeTrigger(Instant.now())) {
            delay(1_000)
        }
        store.saveJob(missingJob)
        store.saveJob(terminalJob)
        store.saveExecution(missing)
        store.saveExecution(terminal)
        store.missingOnLookup.add(missing.id)
        store.terminalOnLookup.add(terminal.id)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(missingJob)
                jobs.add(terminalJob)
                shutdownTimeout = Duration.ofMillis(10)
                heartbeatInterval = Duration.ofMillis(100)
                executionLeaseDuration = Duration.ofMillis(500)
            },
            scope
        )

        scheduler.pollAndExecute()
        delay(30)
        scheduler.stop()
        scope.cancel()

        assertEquals(false, store.updatedStatuses.any { it.second == ExecutionStatus.PENDING })
    }

    @Test
    fun `heartbeat exceptions are caught while job continues`() = runBlocking {
        val store = HeartbeatThrowingStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val execution = JobExecution(jobId = "heartbeat-throws", scheduledAt = Instant.now())
        val job = job("heartbeat-throws", OneTimeTrigger(Instant.now())) {
            delay(80)
        }
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                heartbeatInterval = Duration.ofMillis(10)
                executionLeaseDuration = Duration.ofMillis(100)
            },
            scope
        )

        scheduler.pollAndExecute()
        delay(120)
        scheduler.stop()
        scope.cancel()

        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `heartbeat false cancels active execution`() = runBlocking {
        val store = HeartbeatFalseStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val execution = JobExecution(jobId = "heartbeat-false", scheduledAt = Instant.now())
        val job = job("heartbeat-false", OneTimeTrigger(Instant.now())) {
            delay(500)
        }
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                heartbeatInterval = Duration.ofMillis(10)
                executionLeaseDuration = Duration.ofMillis(100)
            },
            scope
        )

        scheduler.pollAndExecute()
        delay(80)
        scheduler.stop()
        scope.cancel()

        assertEquals(1, store.heartbeatAttempts)
    }

    @Test
    fun `shutdown timeout releases running executions and catches release lookup errors`() = runBlocking {
        val store = ShutdownStore()
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val releasable = JobExecution(jobId = "shutdown-releasable", scheduledAt = Instant.now())
        val claimed = JobExecution(jobId = "shutdown-claimed", scheduledAt = Instant.now())
        val throwing = JobExecution(jobId = "shutdown-throwing", scheduledAt = Instant.now())
        val job = job("shutdown-releasable", OneTimeTrigger(Instant.now())) {
            delay(1_000)
        }
        val claimedJob = job("shutdown-claimed", OneTimeTrigger(Instant.now())) {
            delay(1_000)
        }
        val throwingJob = job("shutdown-throwing", OneTimeTrigger(Instant.now())) {
            delay(1_000)
        }
        store.saveJob(job)
        store.saveJob(claimedJob)
        store.saveJob(throwingJob)
        store.saveExecution(releasable)
        store.saveExecution(claimed)
        store.saveExecution(throwing)
        store.claimedOnLookup.add(claimed.id)
        store.throwOnLookup.add(throwing.id)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                jobs.add(claimedJob)
                jobs.add(throwingJob)
                shutdownTimeout = Duration.ofMillis(10)
                heartbeatInterval = Duration.ofMillis(100)
                executionLeaseDuration = Duration.ofMillis(500)
            },
            scope
        )

        scheduler.pollAndExecute()
        delay(30)
        scheduler.stop()
        scope.cancel()

        assertEquals(ExecutionStatus.PENDING, store.getExecution(releasable.id)?.status)
    }

    @Test
    fun `scheduler loop exits when scope is cancelled before polling begins`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val store = CancelBeforePollingStore()
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job("cancel-before-loop", OneTimeTrigger(Instant.now())))
            },
            scope
        )

        scheduler.start()
        store.saveJobObserved.await()
        delay(50)
        scheduler.stop()
        scope.cancel()

        assertEquals(0, store.pollCalls)
    }

    @Test
    fun `heartbeat loop exits when heartbeat cancels its coroutine`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val store = CancellingHeartbeatStore()
        val execution = JobExecution(jobId = "cancel-heartbeat-loop", scheduledAt = Instant.now())
        val job = job("cancel-heartbeat-loop", OneTimeTrigger(Instant.now())) {
            delay(80)
        }
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
                heartbeatInterval = Duration.ofMillis(10)
                executionLeaseDuration = Duration.ofMillis(100)
            },
            scope
        )

        scheduler.pollAndExecute()
        delay(120)
        scheduler.stop()
        scope.cancel()

        assertEquals(1, store.heartbeatAttempts)
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(execution.id)?.status)
    }

    @Test
    fun `execution without stored correlation id uses execution id in MDC`() = runBlocking {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val store = MockJobStore()
        val execution = JobExecution(
            jobId = "missing-correlation",
            scheduledAt = Instant.now(),
            correlationId = null
        )
        val observedCorrelationId = CompletableDeferred<String?>()
        val job = job("missing-correlation", OneTimeTrigger(Instant.now())) {
            observedCorrelationId.complete(MDC.get("correlationId"))
        }
        store.saveJob(job)
        store.saveExecution(execution)
        val scheduler = Scheduler(
            config {
                this.store = store
                jobs.add(job)
            },
            scope
        )

        scheduler.pollAndExecute()
        val correlationId = observedCorrelationId.await()
        scheduler.stop()
        scope.cancel()

        assertEquals(execution.id.toString(), correlationId)
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(execution.id)?.status)
    }

    private fun config(block: KhronaConfig.() -> Unit = {}) = KhronaConfig().apply {
        store = MockJobStore()
        block()
    }

    private fun job(
        id: String,
        trigger: Trigger,
        handler: JobHandler = {}
    ) = JobDefinition(
        id = id,
        trigger = trigger,
        handler = handler
    )

    private class ThrowingPollStore : MockJobStore() {
        override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> {
            throw IllegalStateException("poll failure")
        }
    }

    private class RecoveringStore : MockJobStore() {
        var recoveryCalls: Int = 0

        override suspend fun resetExpiredExecutions(now: Instant): Int {
            recoveryCalls++
            return 1
        }
    }

    private class MissingJobStore(private val missingJobId: String) : MockJobStore() {
        override suspend fun getJob(jobId: String): JobDefinition? {
            if (jobId == missingJobId) return null
            return super.getJob(jobId)
        }
    }

    private class ClaimFalseStore : MockJobStore() {
        var claimAttempts: Int = 0

        override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
            claimAttempts++
            return false
        }
    }

    private class OtherLockAfterClaimStore : MockJobStore() {
        override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean {
            return excludeExecutionId != null
        }
    }

    private class SupersedeTrackingStore : MockJobStore() {
        var supersedeCalled: Boolean = false

        override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> {
            supersedeCalled = true
            return super.supersedeExecutionsByLockKey(lockKey, excludeExecutionId)
        }
    }

    private class UnknownSupersededStore : MockJobStore() {
        override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> {
            return listOf(UUID.nameUUIDFromBytes("remote-execution".toByteArray()))
        }
    }

    private class RemovingHandlerStore(private val jobIdToRemove: String) : MockJobStore() {
        lateinit var scheduler: Scheduler
        private var removed = false

        override suspend fun getJob(jobId: String): JobDefinition? {
            val job = super.getJob(jobId)
            if (jobId == jobIdToRemove && !removed) {
                removed = true
                val field = Scheduler::class.java.getDeclaredField("handlerRegistry")
                field.isAccessible = true
                val registry = field.get(scheduler) as HandlerRegistry
                registry.remove(jobId)
            }
            return job
        }
    }

    private class HeartbeatThrowingStore : MockJobStore() {
        private var thrown = false

        override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean {
            if (!thrown) {
                thrown = true
                throw IllegalStateException("heartbeat failure")
            }
            return super.heartbeat(id, leaseDuration)
        }
    }

    private class HeartbeatFalseStore : MockJobStore() {
        var heartbeatAttempts: Int = 0

        override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean {
            heartbeatAttempts++
            return false
        }
    }

    private class ShutdownStore : MockJobStore() {
        val throwOnLookup = mutableSetOf<UUID>()
        val claimedOnLookup = mutableSetOf<UUID>()

        override suspend fun getExecution(id: UUID): JobExecution? {
            if (id in throwOnLookup) {
                throw IllegalStateException("lookup failure")
            }
            if (id in claimedOnLookup) {
                return super.getExecution(id)?.copy(status = ExecutionStatus.CLAIMED)
            }
            return super.getExecution(id)
        }
    }

    private class TerminalLookupShutdownStore : MockJobStore() {
        val missingOnLookup = mutableSetOf<UUID>()
        val terminalOnLookup = mutableSetOf<UUID>()

        override suspend fun getExecution(id: UUID): JobExecution? {
            if (id in missingOnLookup) return null
            if (id in terminalOnLookup) return super.getExecution(id)?.copy(status = ExecutionStatus.SUCCESS)
            return super.getExecution(id)
        }
    }

    private class CancelBeforePollingStore : MockJobStore() {
        val saveJobObserved = CompletableDeferred<Unit>()
        var pollCalls: Int = 0

        override suspend fun saveJob(job: JobDefinition) {
            super.saveJob(job)
            saveJobObserved.complete(Unit)
            currentCoroutineContext().cancel()
        }

        override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> {
            pollCalls++
            return super.listEligibleExecutions(now, limit)
        }
    }

    private class CancellingHeartbeatStore : MockJobStore() {
        var heartbeatAttempts: Int = 0

        override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean {
            heartbeatAttempts++
            currentCoroutineContext().cancel()
            return true
        }
    }
}
