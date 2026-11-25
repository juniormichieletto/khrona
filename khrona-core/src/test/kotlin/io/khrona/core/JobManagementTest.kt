@file:OptIn(ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class JobManagementTest {

    private class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    private val json = Json {
        ignoreUnknownKeys = true
        serializersModule = SerializersModule {
            polymorphic(Trigger::class) {
                subclass(IntervalTrigger::class)
                subclass(CronTrigger::class)
                subclass(OneTimeTrigger::class)
            }
        }
    }

    @Test
    fun `JobExecutionQuery validates bounds and evaluates filters and ordering`() = runTest {
        val base = Instant.parse("2030-01-01T10:00:00Z")

        assertThrows(IllegalArgumentException::class.java) { JobExecutionQuery(limit = 0) }
        assertThrows(IllegalArgumentException::class.java) { JobExecutionQuery(limit = -1) }
        assertThrows(IllegalArgumentException::class.java) {
            JobExecutionQuery(scheduledFrom = base.plusSeconds(1), scheduledUntil = base)
        }

        val equalBounds = JobExecutionQuery(limit = 1, scheduledFrom = base, scheduledUntil = base)
        assertEquals(1, equalBounds.limit)
        assertEquals(base, equalBounds.scheduledFrom)
        assertEquals(base, equalBounds.scheduledUntil)

        val store = MockJobStore()
        val (smallerId, largerId) = listOf(UUID.randomUUID(), UUID.randomUUID()).sortedBy { it.toString() }
        val beforeFrom = JobExecution(jobId = "job-a", scheduledAt = base.minusMillis(1), status = ExecutionStatus.SUCCESS)
        val atFrom = JobExecution(jobId = "job-a", scheduledAt = base, status = ExecutionStatus.FAILED)
        val tieSmall = JobExecution(id = smallerId, jobId = "job-a", scheduledAt = base.plusSeconds(5), status = ExecutionStatus.PENDING)
        val tieLarge = JobExecution(id = largerId, jobId = "job-a", scheduledAt = base.plusSeconds(5), status = ExecutionStatus.RUNNING)
        val atUntil = JobExecution(jobId = "job-a", scheduledAt = base.plusSeconds(10), status = ExecutionStatus.CANCELLED)
        val afterUntil = JobExecution(jobId = "job-a", scheduledAt = base.plusSeconds(10).plusMillis(1), status = ExecutionStatus.PENDING)
        val otherJob = JobExecution(jobId = "job-b", scheduledAt = base.plusSeconds(7), status = ExecutionStatus.SUCCESS)

        listOf(beforeFrom, atFrom, tieSmall, tieLarge, atUntil, afterUntil, otherJob).forEach {
            store.saveExecution(it)
        }

        assertTrue(store.listExecutions(JobExecutionQuery(jobId = "job-a", statuses = emptySet())).isEmpty())
        assertEquals(
            listOf(atUntil.id, tieLarge.id, tieSmall.id, atFrom.id),
            store.listExecutions(
                JobExecutionQuery(
                    jobId = "job-a",
                    statuses = null,
                    scheduledFrom = base,
                    scheduledUntil = base.plusSeconds(10)
                )
            ).map { it.id }
        )
        assertEquals(
            listOf(otherJob.id, beforeFrom.id),
            store.listExecutions(JobExecutionQuery(statuses = setOf(ExecutionStatus.SUCCESS))).map { it.id }
        )
        assertEquals(
            listOf(afterUntil.id, atUntil.id),
            store.listExecutions(JobExecutionQuery(jobId = "job-a", limit = 2)).map { it.id }
        )
        assertFalse(equalBounds.matches(beforeFrom))
        assertTrue(equalBounds.matches(atFrom))
        assertFalse(equalBounds.matches(tieSmall))
        assertFalse(JobExecutionQuery(jobId = "job-a").matches(otherJob))
    }

    @Test
    fun `ExecutionStatus isTerminal distinguishes active and terminal states`() {
        val expectedTerminal = setOf(
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.DEAD_LETTERED,
            ExecutionStatus.MISFIRED,
            ExecutionStatus.SUPERSEDED,
            ExecutionStatus.CANCELLED
        )
        ExecutionStatus.entries.forEach { status ->
            assertEquals(status in expectedTerminal, status.isTerminal, "Unexpected isTerminal for $status")
        }
    }

    @Test
    fun `JobDefinition paused defaults to false for legacy payloads and builds via DSL`() {
        val legacyJson = """{"id":"legacy-job","trigger":{"type":"once","executionTime":"2030-01-01T00:00:00Z"}}"""
        val decodedLegacy = json.decodeFromString(JobDefinition.serializer(), legacyJson)
        assertFalse(decodedLegacy.paused)

        val dslPaused = JobBuilder("paused-dsl").apply {
            every(Duration.ofMinutes(1))
            paused = true
            execute {}
        }.build()
        assertTrue(dslPaused.paused)

        val encoded = json.encodeToString(JobDefinition.serializer(), dslPaused)
        val decodedPaused = json.decodeFromString(JobDefinition.serializer(), encoded)
        assertTrue(decodedPaused.paused)
    }

    @Test
    fun `paused job does not execute or misfire pending work`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val runs = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            misfireThreshold = Duration.ofSeconds(5)
            job("paused-job") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                paused = true
                execute { runs.incrementAndGet() }
            }
        }
        val jobDef = config.jobs.single()
        store.saveJob(jobDef)

        val overdueExecution = JobExecution(
            jobId = "paused-job",
            scheduledAt = Instant.now(clock).minusSeconds(60)
        )
        store.saveExecution(overdueExecution)

        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(0, runs.get())
        val persisted = store.getExecution(overdueExecution.id)
        assertEquals(ExecutionStatus.PENDING, persisted?.status)
        assertNull(persisted?.workerId)
    }

    @Test
    fun `paused job does not starve active jobs when pollBatchSize is small`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val pausedRuns = AtomicInteger(0)
        val activeRuns = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            pollBatchSize = 1
            job("paused-oldest") {
                every(Duration.ofMinutes(1))
                paused = true
                execute { pausedRuns.incrementAndGet() }
            }
            job("active-newer") {
                every(Duration.ofMinutes(1))
                execute { activeRuns.incrementAndGet() }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        store.saveExecution(JobExecution(jobId = "paused-oldest", scheduledAt = Instant.EPOCH.minusSeconds(60)))
        store.saveExecution(JobExecution(jobId = "active-newer", scheduledAt = Instant.EPOCH))

        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(0, pausedRuns.get())
        assertEquals(1, activeRuns.get())
    }

    @Test
    fun `pauseJob and resumeJob persist across schedulers and honor misfire policies`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val fireNowRuns = AtomicInteger(0)
        val ignoreRuns = AtomicInteger(0)

        val configA = KhronaConfig().apply {
            this.store = store
            misfireThreshold = Duration.ofSeconds(5)
            job("fire-now-job") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.FIRE_NOW
                execute { fireNowRuns.incrementAndGet() }
            }
            job("ignore-job") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                execute { ignoreRuns.incrementAndGet() }
            }
        }
        configA.jobs.forEach { store.saveJob(it) }

        val schedulerA = Scheduler(configA, backgroundScope, clock)

        // Unknown job and idempotent active resume
        val missingPause = schedulerA.pauseJob("does-not-exist")
        assertFalse(missingPause.success)
        assertNotNull(missingPause.reason)
        val missingResume = schedulerA.resumeJob("does-not-exist")
        assertFalse(missingResume.success)
        assertNotNull(missingResume.reason)

        val alreadyActive = schedulerA.resumeJob("fire-now-job")
        assertTrue(alreadyActive.success)
        assertTrue(alreadyActive.reason!!.contains("active"))

        val pauseResult = schedulerA.pauseJob("fire-now-job")
        assertTrue(pauseResult.success)
        assertNull(pauseResult.reason)
        assertTrue(store.getJob("fire-now-job")!!.paused)

        val pauseAgain = schedulerA.pauseJob("fire-now-job")
        assertTrue(pauseAgain.success)
        assertTrue(pauseAgain.reason!!.contains("paused"))

        assertTrue(schedulerA.pauseJob("ignore-job").success)

        // Second scheduler starts/registers code-defined jobs (paused=false); operational pause must be preserved
        val configB = KhronaConfig().apply {
            this.store = store
            misfireThreshold = Duration.ofSeconds(5)
            job("fire-now-job") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.FIRE_NOW
                execute { fireNowRuns.incrementAndGet() }
            }
            job("ignore-job") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                execute { ignoreRuns.incrementAndGet() }
            }
        }
        val schedulerB = Scheduler(configB, backgroundScope, clock)
        schedulerB.registerJob(configB.jobs[0])
        schedulerB.registerJob(configB.jobs[1])
        assertTrue(store.getJob("fire-now-job")!!.paused)
        assertTrue(store.getJob("ignore-job")!!.paused)

        val overdueFireNow = JobExecution(jobId = "fire-now-job", scheduledAt = Instant.now(clock).minusSeconds(60))
        val overdueIgnore = JobExecution(jobId = "ignore-job", scheduledAt = Instant.now(clock).minusSeconds(60))
        store.saveExecution(overdueFireNow)
        store.saveExecution(overdueIgnore)

        schedulerB.pollAndExecute()
        runCurrent()
        assertEquals(0, fireNowRuns.get())
        assertEquals(0, ignoreRuns.get())
        assertEquals(ExecutionStatus.PENDING, store.getExecution(overdueFireNow.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(overdueIgnore.id)?.status)

        // Resume both and poll -> FIRE_NOW runs, IGNORE marks MISFIRED and schedules next run
        assertTrue(schedulerA.resumeJob("fire-now-job").success)
        assertTrue(schedulerA.resumeJob("ignore-job").success)
        assertFalse(store.getJob("fire-now-job")!!.paused)
        assertFalse(store.getJob("ignore-job")!!.paused)

        schedulerB.pollAndExecute()
        runCurrent()
        assertEquals(1, fireNowRuns.get())
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(overdueFireNow.id)?.status)
        assertEquals(0, ignoreRuns.get())
        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(overdueIgnore.id)?.status)
        val ignorePendingAfterMisfire = store.listExecutions(
            JobExecutionQuery(jobId = "ignore-job", statuses = setOf(ExecutionStatus.PENDING))
        )
        assertEquals(1, ignorePendingAfterMisfire.size)

        // Verify start() also preserves persisted paused state across restart
        assertTrue(schedulerA.pauseJob("fire-now-job").success)
        val schedulerRestarted = Scheduler(
            KhronaConfig().apply {
                this.store = store
                pollingInterval = Duration.ofSeconds(10)
                job("fire-now-job") {
                    every(Duration.ofMinutes(1))
                    execute {}
                }
            },
            backgroundScope,
            clock
        )
        schedulerRestarted.start()
        runCurrent()
        assertTrue(store.getJob("fire-now-job")!!.paused)
        schedulerRestarted.stop()
    }

    @Test
    fun `listJobOverviews and getJobOverview merge persisted and local jobs with derived progress`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val base = Instant.parse("2030-02-01T12:00:00Z")

        val remoteOnlyJob = JobDefinition(
            id = "alpha-remote",
            description = "Persisted on another worker",
            trigger = IntervalTrigger(Duration.ofMinutes(5)),
            paused = true
        )
        val sharedPersistedJob = JobDefinition(
            id = "beta-shared",
            description = "Shared job",
            trigger = IntervalTrigger(Duration.ofMinutes(1)),
            paused = true
        )
        store.saveJob(remoteOnlyJob)
        store.saveJob(sharedPersistedJob)

        val config = KhronaConfig().apply {
            this.store = store
            job("beta-shared") {
                every(Duration.ofMinutes(1))
                paused = false
                execute {}
            }
            job("gamma-local-only") {
                every(Duration.ofMinutes(2))
                execute {}
            }
        }
        val scheduler = Scheduler(config, backgroundScope, clock)

        // Remote-only has zero executions; local-only is not yet persisted in store
        val overviewsInitial = scheduler.listJobOverviews()
        assertEquals(listOf("alpha-remote", "beta-shared", "gamma-local-only"), overviewsInitial.map { it.job.id })

        val alphaInitial = overviewsInitial[0]
        assertTrue(alphaInitial.paused)
        assertFalse(alphaInitial.hasLocalHandler)
        assertNull(alphaInitial.progress.latestExecution)
        assertNull(alphaInitial.progress.nextPendingExecution)
        assertTrue(alphaInitial.progress.activeExecutions.isEmpty())
        assertTrue(alphaInitial.progress.countsByStatus.isEmpty())

        val betaInitial = overviewsInitial[1]
        assertTrue(betaInitial.paused, "Persisted paused=true must take precedence over local paused=false")
        assertTrue(betaInitial.hasLocalHandler)

        val gammaInitial = overviewsInitial[2]
        assertFalse(gammaInitial.paused)
        assertTrue(gammaInitial.hasLocalHandler)

        // Only PENDING executions -> latestExecution falls back to newest pending, nextPendingExecution picks earliest with id tie-break
        val (tieSmallId, tieLargeId) = listOf(UUID.randomUUID(), UUID.randomUUID()).sortedBy { it.toString() }
        val earliestTieSmall = JobExecution(id = tieSmallId, jobId = "gamma-local-only", scheduledAt = base, status = ExecutionStatus.PENDING)
        val earliestTieLarge = JobExecution(id = tieLargeId, jobId = "gamma-local-only", scheduledAt = base, status = ExecutionStatus.PENDING)
        val newerPending = JobExecution(jobId = "gamma-local-only", scheduledAt = base.plusSeconds(60), status = ExecutionStatus.PENDING)
        store.saveExecution(earliestTieSmall)
        store.saveExecution(earliestTieLarge)
        store.saveExecution(newerPending)

        val gammaPendingOnly = scheduler.getJobOverview("gamma-local-only")!!
        assertEquals(newerPending.id, gammaPendingOnly.progress.latestExecution?.id)
        assertEquals(earliestTieSmall.id, gammaPendingOnly.progress.nextPendingExecution?.id)
        assertTrue(gammaPendingOnly.progress.activeExecutions.isEmpty())
        assertEquals(mapOf(ExecutionStatus.PENDING to 3), gammaPendingOnly.progress.countsByStatus)

        // Mixed statuses -> latestExecution prefers newest non-pending
        val oldSuccess = JobExecution(jobId = "beta-shared", scheduledAt = base.minusSeconds(60), status = ExecutionStatus.SUCCESS)
        val failed = JobExecution(jobId = "beta-shared", scheduledAt = base.minusSeconds(30), status = ExecutionStatus.FAILED, error = "boom")
        val claimed = JobExecution(jobId = "beta-shared", scheduledAt = base, status = ExecutionStatus.CLAIMED, workerId = "w-1")
        val running = JobExecution(jobId = "beta-shared", scheduledAt = base.plusSeconds(10), status = ExecutionStatus.RUNNING, workerId = "w-2")
        val nextPending = JobExecution(jobId = "beta-shared", scheduledAt = base.plusSeconds(120), status = ExecutionStatus.PENDING)
        val laterPending = JobExecution(jobId = "beta-shared", scheduledAt = base.plusSeconds(240), status = ExecutionStatus.PENDING)
        listOf(oldSuccess, failed, claimed, running, nextPending, laterPending).forEach { store.saveExecution(it) }

        val betaOverview = scheduler.getJobOverview("beta-shared")!!
        assertEquals(running.id, betaOverview.progress.latestExecution?.id)
        assertEquals(nextPending.id, betaOverview.progress.nextPendingExecution?.id)
        assertEquals(listOf(running.id, claimed.id), betaOverview.progress.activeExecutions.map { it.id })
        assertEquals(
            mapOf(
                ExecutionStatus.PENDING to 2,
                ExecutionStatus.RUNNING to 1,
                ExecutionStatus.CLAIMED to 1,
                ExecutionStatus.FAILED to 1,
                ExecutionStatus.SUCCESS to 1
            ),
            betaOverview.progress.countsByStatus
        )

        assertNull(scheduler.getJobOverview("unknown-job"))
    }

    @Test
    fun `listJobHistory and listExecutions support bounded queries and startJob preserves trigger semantics`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val receivedPayloads = mutableListOf<Any?>()

        val config = KhronaConfig().apply {
            this.store = store
            job("manual-job") {
                once()
                execute { payload -> receivedPayloads.add(payload) }
            }
            job("paused-manual-job") {
                every(Duration.ofHours(1))
                paused = true
                execute { payload -> receivedPayloads.add(payload) }
            }
        }
        config.jobs.forEach { store.saveJob(it) }

        val scheduler = Scheduler(config, backgroundScope, clock)

        val unknownStart = scheduler.startJob("missing-job")
        assertFalse(unknownStart.success)
        assertNotNull(unknownStart.reason)
        assertTrue(unknownStart.affectedExecutionIds.isEmpty())

        assertThrows(IllegalArgumentException::class.java) {
            kotlinx.coroutines.runBlocking { scheduler.trigger("missing-job") }
        }

        val activeStart = scheduler.startJob("manual-job", payload = mapOf("origin" to "facade"))
        assertTrue(activeStart.success)
        assertEquals(1, activeStart.affectedExecutionIds.size)
        val activeExecId = activeStart.affectedExecutionIds.single()

        val pausedStart = scheduler.startJob("paused-manual-job", payload = "deferred")
        assertTrue(pausedStart.success)
        assertEquals(1, pausedStart.affectedExecutionIds.size)
        assertNotNull(pausedStart.reason)
        val pausedExecId = pausedStart.affectedExecutionIds.single()

        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(listOf<Any?>(mapOf("origin" to "facade")), receivedPayloads)
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(activeExecId)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pausedExecId)?.status)

        // trigger() still works and enqueues an additional execution
        scheduler.trigger("manual-job", payload = "via-trigger")
        val historyAll = scheduler.listJobHistory("manual-job")
        assertEquals(2, historyAll.size)
        assertEquals(1, scheduler.listJobHistory("manual-job", limit = 1).size)
        assertThrows(IllegalArgumentException::class.java) {
            kotlinx.coroutines.runBlocking { scheduler.listJobHistory("manual-job", limit = 0) }
        }

        val pendingCrossJob = scheduler.listExecutions(
            JobExecutionQuery(
                statuses = setOf(ExecutionStatus.PENDING),
                scheduledFrom = Instant.EPOCH,
                scheduledUntil = Instant.now(clock).plusSeconds(60)
            )
        )
        assertEquals(2, pendingCrossJob.size)
    }

    @Test
    fun `stopExecution cancels locally active execution persists CANCELLED and releases locks`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val started = CompletableDeferred<UUID>()
        val cancelled = CompletableDeferred<Boolean>()

        val config = KhronaConfig().apply {
            this.store = store
            job("long-job") {
                once()
                concurrencyPolicy = ConcurrencyPolicy.FORBID
                lockKey = "long-job-lock"
                execute {
                    started.complete(executionId)
                    try {
                        awaitCancellation()
                    } catch (e: CancellationException) {
                        cancelled.complete(true)
                        throw e
                    }
                }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        val scheduler = Scheduler(config, backgroundScope, clock)

        val startResult = scheduler.startJob("long-job")
        val execId = startResult.affectedExecutionIds.single()

        scheduler.pollAndExecute()
        runCurrent()
        assertEquals(execId, started.await())
        assertTrue(store.isLockHeld("long-job-lock"))

        val stopResult = scheduler.stopExecution(execId)
        assertTrue(stopResult.success)
        assertEquals(listOf(execId), stopResult.affectedExecutionIds)
        runCurrent()

        assertTrue(withTimeout(1_000) { cancelled.await() })
        val persisted = store.getExecution(execId)
        assertEquals(ExecutionStatus.CANCELLED, persisted?.status)
        assertNotNull(persisted?.completedAt)
        assertFalse(store.isLockHeld("long-job-lock"))
    }

    @Test
    fun `stopExecution rejects unknown terminal pending remote and lost-ownership executions`() = runTest {
        val clock = TestClock(testScheduler)
        val store = object : MockJobStore(clock) {
            var rejectCancelForId: UUID? = null
            override suspend fun updateExecutionStatus(
                id: UUID,
                status: ExecutionStatus,
                error: String?,
                workerId: String?
            ): Boolean {
                if (id == rejectCancelForId && status == ExecutionStatus.CANCELLED) return false
                return super.updateExecutionStatus(id, status, error, workerId)
            }
        }
        val started = CompletableDeferred<UUID>()
        val config = KhronaConfig().apply {
            this.store = store
            job("stoppable-job") {
                once()
                execute {
                    started.complete(executionId)
                    awaitCancellation()
                }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        val scheduler = Scheduler(config, backgroundScope, clock)

        val unknown = scheduler.stopExecution(UUID.randomUUID())
        assertFalse(unknown.success)
        assertTrue(unknown.reason!!.contains("not found", ignoreCase = true))

        val terminalExec = JobExecution(
            jobId = "stoppable-job",
            scheduledAt = Instant.EPOCH,
            status = ExecutionStatus.SUCCESS,
            completedAt = Instant.EPOCH
        )
        store.saveExecution(terminalExec)
        val terminal = scheduler.stopExecution(terminalExec.id)
        assertFalse(terminal.success)
        assertTrue(terminal.reason!!.contains("terminal", ignoreCase = true))

        val pendingExec = JobExecution(
            jobId = "stoppable-job",
            scheduledAt = Instant.now(clock).plusSeconds(60),
            status = ExecutionStatus.PENDING
        )
        store.saveExecution(pendingExec)
        val pending = scheduler.stopExecution(pendingExec.id)
        assertFalse(pending.success)
        assertTrue(pending.reason!!.contains("not active", ignoreCase = true))
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pendingExec.id)?.status)

        val remoteExec = JobExecution(
            jobId = "stoppable-job",
            scheduledAt = Instant.EPOCH,
            status = ExecutionStatus.RUNNING,
            workerId = "remote-worker:1"
        )
        store.saveExecution(remoteExec)
        val remote = scheduler.stopExecution(remoteExec.id)
        assertFalse(remote.success)
        assertTrue(remote.reason!!.contains("not active on this scheduler instance", ignoreCase = true))
        assertEquals(ExecutionStatus.RUNNING, store.getExecution(remoteExec.id)?.status)

        // Locally active, but store rejects ownership transition
        val localId = scheduler.startJob("stoppable-job").affectedExecutionIds.single()
        scheduler.pollAndExecute()
        runCurrent()
        assertEquals(localId, started.await())

        store.rejectCancelForId = localId
        val lostOwner = scheduler.stopExecution(localId)
        assertFalse(lostOwner.success)
        assertTrue(lostOwner.affectedExecutionIds.isEmpty())
        assertNotNull(lostOwner.reason)

        store.rejectCancelForId = null
        assertTrue(scheduler.stopExecution(localId).success)
        runCurrent()
    }

    @Test
    fun `stopJob stops all locally active executions and handles unknown idle and partial-failure cases`() = runTest {
        val clock = TestClock(testScheduler)
        val store = object : MockJobStore(clock) {
            var failCancelId: UUID? = null
            override suspend fun updateExecutionStatus(
                id: UUID,
                status: ExecutionStatus,
                error: String?,
                workerId: String?
            ): Boolean {
                if (id == failCancelId && status == ExecutionStatus.CANCELLED) return false
                return super.updateExecutionStatus(id, status, error, workerId)
            }
        }
        val activeStarted = mutableListOf<UUID>()
        val bothStarted = CompletableDeferred<Unit>()
        val config = KhronaConfig().apply {
            this.store = store
            job("parallel-job") {
                every(Duration.ofHours(1))
                concurrencyPolicy = ConcurrencyPolicy.ALLOW
                execute {
                    activeStarted.add(executionId)
                    if (activeStarted.size == 2) bothStarted.complete(Unit)
                    awaitCancellation()
                }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        val scheduler = Scheduler(config, backgroundScope, clock)

        val unknownStop = scheduler.stopJob("unknown-job")
        assertFalse(unknownStop.success)
        assertTrue(unknownStop.reason!!.contains("not found", ignoreCase = true))

        val idleStop = scheduler.stopJob("parallel-job")
        assertFalse(idleStop.success)
        assertTrue(idleStop.reason!!.contains("no active", ignoreCase = true))

        // Job defined in config.jobs but not yet persisted in store
        val unpersistedScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                job("config-only-idle") {
                    once()
                    execute {}
                }
            },
            backgroundScope,
            clock
        )
        val configOnlyIdle = unpersistedScheduler.stopJob("config-only-idle")
        assertFalse(configOnlyIdle.success)
        assertTrue(configOnlyIdle.reason!!.contains("no active", ignoreCase = true))

        val firstId = scheduler.startJob("parallel-job").affectedExecutionIds.single()
        val secondId = scheduler.startJob("parallel-job").affectedExecutionIds.single()
        scheduler.pollAndExecute()
        runCurrent()
        bothStarted.await()

        // Partial stop: failCancelId fails, other succeeds
        store.failCancelId = firstId
        val partial = scheduler.stopJob("parallel-job")
        assertTrue(partial.success)
        assertEquals(listOf(secondId), partial.affectedExecutionIds)
        assertNotNull(partial.reason)
        assertEquals(ExecutionStatus.CANCELLED, store.getExecution(secondId)?.status)

        // All remaining active fail to transition -> returns success = false
        val allFailed = scheduler.stopJob("parallel-job")
        assertFalse(allFailed.success)
        assertTrue(allFailed.affectedExecutionIds.isEmpty())

        // Clean up remaining active execution
        store.failCancelId = null
        val fullStop = scheduler.stopJob("parallel-job")
        assertTrue(fullStop.success)
        assertEquals(listOf(firstId), fullStop.affectedExecutionIds)
        runCurrent()
        assertEquals(ExecutionStatus.CANCELLED, store.getExecution(firstId)?.status)

        // Verify full multi-execution stop
        activeStarted.clear()
        val bothAgain = CompletableDeferred<Unit>()
        val configMulti = KhronaConfig().apply {
            this.store = store
            job("multi-stop-job") {
                every(Duration.ofHours(1))
                concurrencyPolicy = ConcurrencyPolicy.ALLOW
                execute {
                    activeStarted.add(executionId)
                    if (activeStarted.size == 2) bothAgain.complete(Unit)
                    awaitCancellation()
                }
            }
        }
        configMulti.jobs.forEach { store.saveJob(it) }
        val multiScheduler = Scheduler(configMulti, backgroundScope, clock)
        val m1 = multiScheduler.startJob("multi-stop-job").affectedExecutionIds.single()
        val m2 = multiScheduler.startJob("multi-stop-job").affectedExecutionIds.single()
        multiScheduler.pollAndExecute()
        runCurrent()
        bothAgain.await()

        val multiResult = multiScheduler.stopJob("multi-stop-job")
        assertTrue(multiResult.success)
        assertNull(multiResult.reason)
        assertEquals(setOf(m1, m2), multiResult.affectedExecutionIds.toSet())
        runCurrent()
        assertEquals(ExecutionStatus.CANCELLED, store.getExecution(m1)?.status)
        assertEquals(ExecutionStatus.CANCELLED, store.getExecution(m2)?.status)
    }

    @Test
    fun `ExecutionContext exposes stable identifiers across retries and lease reclaims and normal checkpoint continues`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        data class SeenContext(
            val executionId: UUID,
            val jobId: String,
            val attempt: Int,
            val scheduledAt: Instant,
            val payload: Any?,
            val correlationId: String
        )
        val seen = mutableListOf<SeenContext>()
        val shouldFailFirstAttempt = AtomicBoolean(true)
        val afterCheckpointRan = AtomicBoolean(false)

        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofMinutes(5)
            heartbeatInterval = Duration.ofMinutes(1)
            job("ctx-job") {
                once()
                retry {
                    maxAttempts = 2
                    initialDelay = Duration.ofSeconds(1)
                    maxDelay = Duration.ofSeconds(1)
                    jitter = 0.0
                }
                execute { payload ->
                    checkpoint()
                    seen.add(
                        SeenContext(
                            executionId = executionId,
                            jobId = jobId,
                            attempt = attempt,
                            scheduledAt = scheduledAt,
                            payload = this.payload,
                            correlationId = correlationId
                        )
                    )
                    if (shouldFailFirstAttempt.compareAndSet(true, false)) {
                        throw IllegalStateException("boom on attempt 0")
                    }
                    checkpoint()
                    afterCheckpointRan.set(true)
                }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        val scheduler = Scheduler(config, backgroundScope, clock)

        val initialExecId = scheduler.startJob("ctx-job", payload = "item-1").affectedExecutionIds.single()
        val initialExec = store.getExecution(initialExecId)!!

        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(1, seen.size)
        assertEquals(initialExecId, seen[0].executionId)
        assertEquals("ctx-job", seen[0].jobId)
        assertEquals(0, seen[0].attempt)
        assertEquals(initialExec.scheduledAt, seen[0].scheduledAt)
        assertEquals("item-1", seen[0].payload)
        assertEquals(initialExec.correlationId, seen[0].correlationId)
        assertFalse(afterCheckpointRan.get())

        // Retry creates a new executionId with attempt == 1 and same correlationId
        advanceTimeBy(1_500)
        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(2, seen.size)
        assertTrue(seen[1].executionId != initialExecId)
        assertEquals(1, seen[1].attempt)
        assertEquals(initialExec.correlationId, seen[1].correlationId)
        assertTrue(afterCheckpointRan.get())
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(seen[1].executionId)?.status)

        // Lease reclaim preserves the same executionId and attempt
        val reclaimTarget = JobExecution(
            jobId = "ctx-job",
            scheduledAt = Instant.now(clock).minusSeconds(10),
            attempt = 0,
            payload = "reclaimed-item"
        )
        store.saveExecution(reclaimTarget)
        assertTrue(store.claimExecution(reclaimTarget.id, "dead-worker", Duration.ofSeconds(-5)))

        scheduler.pollAndExecute()
        runCurrent()

        assertEquals(3, seen.size)
        assertEquals(reclaimTarget.id, seen[2].executionId)
        assertEquals(0, seen[2].attempt)
        assertEquals("reclaimed-item", seen[2].payload)
    }

    @Test
    fun `shutdown checkpoint stops handler cleanly releases execution to PENDING and preserves timeout and manual stop semantics`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val entered = CompletableDeferred<Unit>()
        val proceedToCheckpoint = CompletableDeferred<Unit>()
        val afterCheckpoint = AtomicBoolean(false)

        val config = KhronaConfig().apply {
            this.store = store
            pollingInterval = Duration.ofSeconds(10)
            shutdownTimeout = Duration.ofSeconds(10)
            job("checkpoint-job") {
                once()
                timeout = Duration.ofSeconds(30)
                execute {
                    entered.complete(Unit)
                    proceedToCheckpoint.await()
                    checkpoint()
                    afterCheckpoint.set(true)
                }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        val scheduler = Scheduler(config, backgroundScope, clock)
        val execId = scheduler.startJob("checkpoint-job").affectedExecutionIds.single()

        scheduler.start()
        runCurrent()
        entered.await()

        val stopStartTime = testScheduler.currentTime
        val stopDeferred = async { scheduler.stop() }
        runCurrent()
        proceedToCheckpoint.complete(Unit)
        stopDeferred.await()
        val elapsedStopMs = testScheduler.currentTime - stopStartTime

        assertTrue(elapsedStopMs < 1_000, "Checkpoint shutdown should finish within grace period, took ${elapsedStopMs}ms")
        assertFalse(afterCheckpoint.get(), "Code after checkpoint() must not execute during shutdown")

        val released = store.getExecution(execId)!!
        assertEquals(ExecutionStatus.PENDING, released.status)
        assertNull(released.workerId)
        assertNull(released.startedAt)
        assertNull(released.expiresAt)
        assertNull(released.completedAt)
        assertEquals(listOf(execId), store.listEligibleExecutions(Instant.now(clock)).map { it.id })

        // Genuine timeout still fails/retries as usual even when checkpoint() is used inside handler
        val timeoutAttempts = AtomicInteger(0)
        val timeoutScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                job("timeout-job") {
                    once()
                    timeout = Duration.ofMillis(50)
                    retry { maxAttempts = 1 }
                    execute {
                        timeoutAttempts.incrementAndGet()
                        checkpoint()
                        delay(500)
                    }
                }
            },
            backgroundScope,
            clock
        )
        timeoutScheduler.config.jobs.forEach { store.saveJob(it) }
        val timeoutExecId = timeoutScheduler.startJob("timeout-job").affectedExecutionIds.single()
        timeoutScheduler.pollAndExecute()
        advanceTimeBy(100)
        runCurrent()
        assertEquals(1, timeoutAttempts.get())
        assertEquals(ExecutionStatus.DEAD_LETTERED, store.getExecution(timeoutExecId)?.status)

        // Manual stopExecution() + checkpoint() preserves CANCELLED rather than releasing to PENDING
        val stopStarted = CompletableDeferred<Unit>()
        val stopCheckpoint = CompletableDeferred<Unit>()
        val manualStopScheduler = Scheduler(
            KhronaConfig().apply {
                this.store = store
                job("manual-stop-checkpoint") {
                    once()
                    execute {
                        stopStarted.complete(Unit)
                        stopCheckpoint.await()
                        checkpoint()
                    }
                }
            },
            backgroundScope,
            clock
        )
        manualStopScheduler.config.jobs.forEach { store.saveJob(it) }
        val manualStopId = manualStopScheduler.startJob("manual-stop-checkpoint").affectedExecutionIds.single()
        manualStopScheduler.pollAndExecute()
        runCurrent()
        stopStarted.await()

        assertTrue(manualStopScheduler.stopExecution(manualStopId).success)
        stopCheckpoint.complete(Unit)
        runCurrent()
        assertEquals(ExecutionStatus.CANCELLED, store.getExecution(manualStopId)?.status)
    }

    @Test
    fun `checkpoint prioritizes coroutine cancellation over stopping and logs transient release errors`() = runTest {
        // Direct ExecutionContext check: already cancelled coroutine with isStopping=true throws regular CancellationException
        val cancelledJob = launch {
            val ctx = ExecutionContext(
                executionId = UUID.randomUUID(),
                jobId = "direct-ctx",
                attempt = 0,
                scheduledAt = Instant.EPOCH,
                payload = null,
                correlationId = "corr-1",
                isStopping = { true }
            )
            coroutineContext.job.cancel(CancellationException("manually cancelled"))
            var thrown: Throwable? = null
            try {
                ctx.checkpoint()
            } catch (e: Throwable) {
                thrown = e
            }
            assertTrue(thrown is CancellationException)
            assertFalse(thrown is ShutdownCheckpointException)
        }
        cancelledJob.join()

        // Transient store exception releasing execution after shutdown checkpoint does not crash scheduler.stop()
        val clock = TestClock(testScheduler)
        val failingReleaseStore = object : MockJobStore(clock) {
            override suspend fun updateExecutionStatus(
                id: UUID,
                status: ExecutionStatus,
                error: String?,
                workerId: String?
            ): Boolean {
                if (status == ExecutionStatus.PENDING) {
                    throw IllegalStateException("transient DB error on release")
                }
                return super.updateExecutionStatus(id, status, error, workerId)
            }
        }
        val entered = CompletableDeferred<Unit>()
        val proceed = CompletableDeferred<Unit>()
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = failingReleaseStore
                shutdownTimeout = Duration.ofSeconds(5)
                job("failing-release") {
                    once()
                    execute {
                        entered.complete(Unit)
                        proceed.await()
                        checkpoint()
                    }
                }
            },
            backgroundScope,
            clock
        )
        scheduler.config.jobs.forEach { failingReleaseStore.saveJob(it) }
        scheduler.startJob("failing-release")
        scheduler.start()
        runCurrent()
        entered.await()

        val stopDeferred = async { scheduler.stop() }
        runCurrent()
        proceed.complete(Unit)
        stopDeferred.await()
    }

    @Test
    fun `poll racing with pauseJob does not claim execute or supersede after pause is persisted`() = runTest {
        val clock = TestClock(testScheduler)
        val backingStore = MockJobStore(clock)
        val runs = AtomicInteger(0)
        val pauseTriggered = AtomicBoolean(false)
        lateinit var schedulerB: Scheduler
        var pauseResult: JobControlResult? = null

        val interceptingStore = object : JobStore by backingStore {
            override suspend fun getJob(jobId: String): JobDefinition? {
                val jobDef = backingStore.getJob(jobId)
                if (jobId == "race-replace-job" && pauseTriggered.compareAndSet(false, true)) {
                    pauseResult = schedulerB.pauseJob(jobId)
                }
                return jobDef
            }
        }

        val configA = KhronaConfig().apply {
            this.store = interceptingStore
            job("race-replace-job") {
                every(Duration.ofMinutes(1))
                concurrencyPolicy = ConcurrencyPolicy.REPLACE
                lockKey = "pause-race-lock"
                execute { runs.incrementAndGet() }
            }
        }
        val configB = KhronaConfig().apply {
            this.store = backingStore
        }

        backingStore.saveJob(configA.jobs.single())
        val existingRunning = JobExecution(
            jobId = "race-replace-job",
            scheduledAt = Instant.now(clock).minusSeconds(30),
            status = ExecutionStatus.RUNNING,
            workerId = "existing-worker",
            expiresAt = Instant.now(clock).plusSeconds(300),
            lockKey = "pause-race-lock"
        )
        val duePending = JobExecution(
            jobId = "race-replace-job",
            scheduledAt = Instant.now(clock).minusSeconds(1),
            status = ExecutionStatus.PENDING,
            lockKey = "pause-race-lock"
        )
        backingStore.saveExecution(existingRunning)
        backingStore.saveExecution(duePending)

        val schedulerA = Scheduler(configA, backgroundScope, clock)
        schedulerB = Scheduler(configB, backgroundScope, clock)

        schedulerA.pollAndExecute()
        runCurrent()

        assertTrue(pauseTriggered.get())
        assertTrue(pauseResult?.success == true)
        assertEquals(0, runs.get())
        val pendingAfter = backingStore.getExecution(duePending.id)
        assertEquals(ExecutionStatus.PENDING, pendingAfter?.status)
        assertNull(pendingAfter?.workerId)
        assertEquals(ExecutionStatus.RUNNING, backingStore.getExecution(existingRunning.id)?.status)
    }

    @Test
    fun `misfire IGNORE poll racing with pauseJob does not claim or mark MISFIRED`() = runTest {
        val clock = TestClock(testScheduler)
        clock.let { advanceTimeBy(120_000) }
        val backingStore = MockJobStore(clock)
        val runs = AtomicInteger(0)
        val pauseTriggered = AtomicBoolean(false)
        lateinit var schedulerB: Scheduler
        var pauseResult: JobControlResult? = null

        val interceptingStore = object : JobStore by backingStore {
            override suspend fun getJob(jobId: String): JobDefinition? {
                val jobDef = backingStore.getJob(jobId)
                if (jobId == "race-misfire-ignore" && pauseTriggered.compareAndSet(false, true)) {
                    pauseResult = schedulerB.pauseJob(jobId)
                }
                return jobDef
            }
        }

        val configA = KhronaConfig().apply {
            this.store = interceptingStore
            misfireThreshold = Duration.ofSeconds(5)
            job("race-misfire-ignore") {
                every(Duration.ofMinutes(1))
                misfirePolicy = MisfirePolicy.IGNORE
                execute { runs.incrementAndGet() }
            }
        }
        val configB = KhronaConfig().apply {
            this.store = backingStore
        }

        backingStore.saveJob(configA.jobs.single())
        val overdue = JobExecution(
            jobId = "race-misfire-ignore",
            scheduledAt = Instant.now(clock).minusSeconds(60)
        )
        backingStore.saveExecution(overdue)

        val schedulerA = Scheduler(configA, backgroundScope, clock)
        schedulerB = Scheduler(configB, backgroundScope, clock)

        schedulerA.pollAndExecute()
        runCurrent()

        assertTrue(pauseTriggered.get())
        assertTrue(pauseResult?.success == true)
        assertEquals(0, runs.get())
        assertEquals(ExecutionStatus.PENDING, backingStore.getExecution(overdue.id)?.status)
        assertEquals(1, backingStore.listExecutions(JobExecutionQuery(jobId = "race-misfire-ignore")).size)
    }
}
