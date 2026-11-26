package io.khrona.core.testing

import io.khrona.core.ExecutionStatus
import io.khrona.core.HealthStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobExecutionQuery
import io.khrona.core.JobStore
import io.khrona.core.IntervalTrigger
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID

interface JobStoreContract {
    fun createStore(): JobStore

    @Test
    fun `stores and retrieves job definitions`() = runTest {
        val store = createStore()
        val job = JobDefinition(
            id = "contract-job",
            description = "Contract job",
            handler = {},
            trigger = IntervalTrigger(Duration.ofMinutes(1))
        )

        store.saveJob(job)

        val saved = store.getJob(job.id)
        assertNotNull(saved)
        assertEquals(job.id, saved?.id)
        assertEquals(job.description, saved?.description)
        assertTrue(saved?.trigger is IntervalTrigger)
    }

    @Test
    fun `lists only due pending executions with a bounded limit`() = runTest {
        val store = createStore()
        val testRunId = UUID.randomUUID()
        val now = Instant.parse("2030-01-01T00:00:00Z")
        val due1 = JobExecution(jobId = "contract-due-1-$testRunId", scheduledAt = now.minusSeconds(2))
        val due2 = JobExecution(jobId = "contract-due-2-$testRunId", scheduledAt = now.minusSeconds(1))
        val future = JobExecution(jobId = "contract-future-$testRunId", scheduledAt = now.plusSeconds(1))

        store.saveExecution(due1)
        store.saveExecution(due2)
        store.saveExecution(future)

        val eligible = store.listEligibleExecutions(now, limit = Int.MAX_VALUE)
            .filter { it.id == due1.id || it.id == due2.id || it.id == future.id }

        assertEquals(setOf(due1.id, due2.id), eligible.map { it.id }.toSet())
    }

    @Test
    fun `claims an execution only once`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "claim-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        val claimed = store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5))
        val claimedAgain = store.claimExecution(execution.id, "worker-2", Duration.ofMinutes(5))

        assertTrue(claimed)
        assertFalse(claimedAgain)

        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CLAIMED, updated?.status)
        assertEquals("worker-1", updated?.workerId)
    }

    @Test
    fun `updates terminal execution status`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "status-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)

        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.SUCCESS, updated?.status)
        assertNotNull(updated?.completedAt)
    }

    @Test
    fun `expired claims become eligible and reclaimable`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "lease-job", scheduledAt = Instant.now().minusSeconds(60))
        store.saveExecution(execution)

        val claimed = store.claimExecution(execution.id, "worker-1", Duration.ofSeconds(-10))
        assertTrue(claimed)

        val eligible = store.listEligibleExecutions(Instant.now())
            .filter { it.id == execution.id }
        assertEquals(1, eligible.size)
        assertEquals(execution.id, eligible.single().id)

        val reclaimed = store.claimExecution(execution.id, "worker-2", Duration.ofMinutes(5))
        assertTrue(reclaimed)
        assertEquals("worker-2", store.getExecution(execution.id)?.workerId)
    }

    @Test
    fun `lock is held only while execution is active`() = runTest {
        val store = createStore()
        val lockKey = "contract-lock"
        val execution = JobExecution(jobId = "lock-job", scheduledAt = Instant.now(), lockKey = lockKey)
        store.saveExecution(execution)

        assertFalse(store.isLockHeld(lockKey))

        store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5))
        assertTrue(store.isLockHeld(lockKey))

        store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
        assertFalse(store.isLockHeld(lockKey))
    }

    @Test
    fun `heartbeat extends active lease`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "heartbeat-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        store.claimExecution(execution.id, "worker-1", Duration.ofSeconds(5))
        val firstExpiry = store.getExecution(execution.id)?.expiresAt

        val heartbeated = store.heartbeat(execution.id, "worker-1", Duration.ofMinutes(1))
        val secondExpiry = store.getExecution(execution.id)?.expiresAt

        assertTrue(heartbeated)
        assertNotNull(firstExpiry)
        assertNotNull(secondExpiry)
        assertTrue(secondExpiry!! > firstExpiry!!)
    }

    @Test
    fun `heartbeat rejects a former worker after another worker reclaims the execution`() = runTest {
        val store = createStore()
        val execution = JobExecution(
            jobId = "reclaimed-heartbeat-job",
            scheduledAt = Instant.now()
        )
        store.saveExecution(execution)

        assertTrue(store.claimExecution(execution.id, "old-worker", Duration.ofSeconds(-5)))
        assertTrue(store.claimExecution(execution.id, "new-worker", Duration.ofMinutes(1)))

        assertFalse(
            store.heartbeat(execution.id, "old-worker", Duration.ofMinutes(1)),
            "A former worker must not extend the lease owned by the new worker"
        )
        assertEquals("new-worker", store.getExecution(execution.id)?.workerId)
    }

    @Test
    fun `heartbeat rejects an expired lease even before reclamation`() = runTest {
        val store = createStore()
        val execution = JobExecution(
            jobId = "expired-heartbeat-job",
            scheduledAt = Instant.now()
        )
        store.saveExecution(execution)

        assertTrue(store.claimExecution(execution.id, "worker-1", Duration.ofSeconds(-5)))
        val expiredLease = store.getExecution(execution.id)?.expiresAt

        assertFalse(
            store.heartbeat(execution.id, "worker-1", Duration.ofMinutes(1)),
            "An expired lease must not be revived by heartbeat even before another worker reclaims it"
        )
        assertEquals(expiredLease, store.getExecution(execution.id)?.expiresAt)
    }

    @Test
    fun `updateExecutionStatus rejects stale worker ownership and non-active states when workerId is supplied`() = runTest {
        val store = createStore()
        val reclaimed = JobExecution(
            jobId = "fenced-reclaimed-status-job",
            scheduledAt = Instant.now(),
            lockKey = "fenced-reclaimed-lock"
        )
        val superseded = JobExecution(
            jobId = "fenced-superseded-status-job",
            scheduledAt = Instant.now(),
            lockKey = "fenced-superseded-lock"
        )
        store.saveExecution(reclaimed)
        store.saveExecution(superseded)

        assertTrue(store.claimExecution(reclaimed.id, "old-worker", Duration.ofSeconds(-5)))
        assertTrue(store.claimExecution(reclaimed.id, "new-worker", Duration.ofMinutes(1)))

        assertFalse(
            store.updateExecutionStatus(reclaimed.id, ExecutionStatus.SUCCESS, workerId = "old-worker"),
            "Stale worker must not overwrite reclaimed execution status"
        )
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(reclaimed.id)?.status)
        assertEquals("new-worker", store.getExecution(reclaimed.id)?.workerId)
        assertTrue(store.isLockHeld("fenced-reclaimed-lock"))

        assertTrue(store.updateExecutionStatus(reclaimed.id, ExecutionStatus.RUNNING, workerId = "new-worker"))
        assertEquals(ExecutionStatus.RUNNING, store.getExecution(reclaimed.id)?.status)

        assertTrue(store.updateExecutionStatus(reclaimed.id, ExecutionStatus.PENDING, error = "Released", workerId = "new-worker"))
        val released = store.getExecution(reclaimed.id)
        assertEquals(ExecutionStatus.PENDING, released?.status)
        assertNull(released?.workerId)
        assertNull(released?.expiresAt)
        assertNull(released?.startedAt)

        assertTrue(store.claimExecution(superseded.id, "worker-a", Duration.ofMinutes(1)))
        assertEquals(listOf(superseded.id), store.supersedeExecutionsByLockKey("fenced-superseded-lock"))
        assertFalse(
            store.updateExecutionStatus(superseded.id, ExecutionStatus.RUNNING, workerId = "worker-a"),
            "Superseded execution must not transition back to RUNNING"
        )
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(superseded.id)?.status)
    }

    @Test
    fun `purges terminal executions completed before cutoff up to limit`() = runTest {
        val store = createStore()
        val testRunId = UUID.randomUUID()
        val cutoff = Instant.parse("2030-01-01T12:00:00Z")
        store.cleanupCompletedExecutions(before = Instant.now().plusSeconds(3600), limit = 100_000)

        val oldSuccess = JobExecution(jobId = "cleanup-succ-$testRunId", scheduledAt = cutoff.minusSeconds(100))
        val oldSuperseded = JobExecution(jobId = "cleanup-super-$testRunId", scheduledAt = cutoff.minusSeconds(90))
        val oldMisfired = JobExecution(jobId = "cleanup-misfired-$testRunId", scheduledAt = cutoff.minusSeconds(80))
        val oldDeadLetter = JobExecution(jobId = "cleanup-dead-$testRunId", scheduledAt = cutoff.minusSeconds(70))
        val activePending = JobExecution(jobId = "cleanup-pending-$testRunId", scheduledAt = cutoff.minusSeconds(60))

        val all = listOf(oldSuccess, oldSuperseded, oldMisfired, oldDeadLetter, activePending)
        all.forEach { store.saveExecution(it) }

        store.updateExecutionStatus(oldSuccess.id, ExecutionStatus.SUCCESS)
        store.updateExecutionStatus(oldSuperseded.id, ExecutionStatus.SUPERSEDED)
        store.updateExecutionStatus(oldMisfired.id, ExecutionStatus.MISFIRED)
        store.updateExecutionStatus(oldDeadLetter.id, ExecutionStatus.DEAD_LETTERED)

        // Cutoff is set in the future of the completion times, so old executions are older than cutoff.
        val futureCutoff = Instant.now().plusSeconds(60)

        // Clean up only SUCCESS, SUPERSEDED, MISFIRED with limit = 2
        val cleanedBatch1 = store.cleanupCompletedExecutions(
            before = futureCutoff,
            statuses = setOf(ExecutionStatus.SUCCESS, ExecutionStatus.SUPERSEDED, ExecutionStatus.MISFIRED),
            limit = 2
        )
        assertEquals(2, cleanedBatch1)

        // Clean up remaining
        val cleanedBatch2 = store.cleanupCompletedExecutions(
            before = futureCutoff,
            statuses = setOf(ExecutionStatus.SUCCESS, ExecutionStatus.SUPERSEDED, ExecutionStatus.MISFIRED),
            limit = 10
        )
        assertEquals(1, cleanedBatch2)

        // Ensure target old executions were removed
        val deletedCount = listOf(oldSuccess, oldSuperseded, oldMisfired).count { store.getExecution(it.id) == null }
        assertEquals(3, deletedCount)

        // Ensure DEAD_LETTERED and PENDING were preserved
        assertNotNull(store.getExecution(oldDeadLetter.id), "DEAD_LETTERED execution should not be cleaned up")
        assertNotNull(store.getExecution(activePending.id), "PENDING execution should not be cleaned up")

        // Zero limit or empty statuses should do nothing
        assertEquals(0, store.cleanupCompletedExecutions(before = futureCutoff, statuses = emptySet()))
        assertEquals(0, store.cleanupCompletedExecutions(before = futureCutoff, statuses = setOf(ExecutionStatus.DEAD_LETTERED), limit = 0))
    }

    @Test
    fun `saveExecution with PENDING does not overwrite an already claimed running or completed execution`() = runTest {
        val store = createStore()
        val now = Instant.now()
        val claimedExec = JobExecution(
            jobId = "contract-race-claimed-${UUID.randomUUID()}",
            scheduledAt = now.minusSeconds(1),
            status = ExecutionStatus.PENDING
        )
        store.saveExecution(claimedExec)
        assertTrue(store.claimExecution(claimedExec.id, "worker-a", Duration.ofMinutes(5)))

        // Late startup saveExecution with PENDING must not overwrite CLAIMED
        store.saveExecution(claimedExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(claimedExec.id)?.status)
        assertEquals("worker-a", store.getExecution(claimedExec.id)?.workerId)

        // Transition to SUCCESS and verify late startup saveExecution with PENDING does not overwrite terminal status
        assertTrue(store.updateExecutionStatus(claimedExec.id, ExecutionStatus.SUCCESS, workerId = "worker-a"))
        store.saveExecution(claimedExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(claimedExec.id)?.status)
    }

    @Test
    fun `seedInitialExecution skips seeding when another non-terminal execution exists for the job`() = runTest {
        val store = createStore()
        val jobId = "contract-seed-job-${UUID.randomUUID()}"
        val job = JobDefinition(
            id = jobId,
            handler = {},
            trigger = IntervalTrigger(Duration.ofHours(1))
        )
        store.saveJob(job)

        val firstTime = Instant.parse("2030-01-01T10:00:00Z")
        val secondTime = Instant.parse("2030-01-01T10:00:05Z")
        val firstExec = JobExecution(jobId = jobId, scheduledAt = firstTime)
        val secondExec = JobExecution(jobId = jobId, scheduledAt = secondTime)

        assertTrue(store.seedInitialExecution(firstExec))
        assertFalse(store.seedInitialExecution(firstExec), "Same execution ID must not be seeded twice")
        assertFalse(store.seedInitialExecution(secondExec), "Second chain must not be seeded while first is PENDING")
        assertNull(store.getExecution(secondExec.id))

        assertTrue(store.claimExecution(firstExec.id, "worker-1", Duration.ofMinutes(5)))
        assertFalse(store.seedInitialExecution(secondExec), "Second chain must not be seeded while first is CLAIMED")

        assertTrue(store.updateExecutionStatus(firstExec.id, ExecutionStatus.RUNNING, workerId = "worker-1"))
        assertFalse(store.seedInitialExecution(secondExec), "Second chain must not be seeded while first is RUNNING")

        // Complete and purge the execution via retention cleanup; re-saving the job and re-seeding the same deterministic ID must still be skipped
        assertTrue(store.updateExecutionStatus(firstExec.id, ExecutionStatus.SUCCESS, workerId = "worker-1"))
        val purged = store.cleanupCompletedExecutions(
            before = Instant.now().plusSeconds(3600),
            statuses = setOf(ExecutionStatus.SUCCESS),
            limit = 10_000
        )
        assertTrue(purged >= 1)
        assertNull(store.getExecution(firstExec.id))
        store.saveJob(job)
        assertFalse(
            store.seedInitialExecution(firstExec),
            "Previously seeded deterministic execution must not be re-seeded after retention cleanup and restart"
        )
    }

    @Test
    fun `listEligibleExecutions filters by jobIds before applying limit`() = runTest {
        val store = createStore()
        val now = Instant.now()
        val foreignJobId = "contract-foreign-${UUID.randomUUID()}"
        val localJobId = "contract-local-${UUID.randomUUID()}"

        repeat(3) { idx ->
            store.saveExecution(
                JobExecution(
                    jobId = foreignJobId,
                    scheduledAt = now.minusSeconds(100L - idx)
                )
            )
        }
        val localExec = JobExecution(
            jobId = localJobId,
            scheduledAt = now.minusSeconds(10)
        )
        store.saveExecution(localExec)

        assertTrue(store.listEligibleExecutions(now, limit = 0, jobIds = setOf(localJobId)).isEmpty())
        assertTrue(store.listEligibleExecutions(now, limit = 2, jobIds = emptySet()).isEmpty())

        val filtered = store.listEligibleExecutions(now, limit = 1, jobIds = setOf(localJobId))
        assertEquals(listOf(localExec.id), filtered.map { it.id })
    }

    @Test
    fun `completeExecution atomically updates status and schedules follow-up when ownership matches`() = runTest {
        val store = createStore()
        val now = Instant.now()
        val jobId = "contract-complete-${UUID.randomUUID()}"
        val current = JobExecution(jobId = jobId, scheduledAt = now.minusSeconds(10))
        val followUp = JobExecution(jobId = jobId, scheduledAt = now.plusSeconds(60))
        store.saveExecution(current)

        assertFalse(
            store.completeExecution(
                id = UUID.randomUUID(),
                status = ExecutionStatus.SUCCESS,
                workerId = "worker-1",
                nextExecution = followUp
            )
        )
        assertNull(store.getExecution(followUp.id))

        assertTrue(store.claimExecution(current.id, "worker-1", Duration.ofMinutes(5)))
        assertFalse(
            store.completeExecution(
                id = current.id,
                status = ExecutionStatus.SUCCESS,
                workerId = "wrong-worker",
                nextExecution = followUp
            ),
            "Mismatched workerId must not complete execution or schedule follow-up"
        )
        assertNull(store.getExecution(followUp.id))

        assertTrue(
            store.completeExecution(
                id = current.id,
                status = ExecutionStatus.SUCCESS,
                workerId = "worker-1",
                nextExecution = followUp
            )
        )
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(current.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(followUp.id)?.status)
    }

    @Test
    fun `seedInitialExecution backfills completion marker for legacy completed execution before retention cleanup`() = runTest {
        val store = createStore()
        val jobId = "once-upgrade-${UUID.randomUUID()}"
        val job = JobDefinition(id = jobId, trigger = io.khrona.core.OneTimeTrigger(Instant.EPOCH))
        val execution = JobExecution(jobId = jobId, scheduledAt = Instant.EPOCH)

        // Simulate a job and completed execution persisted prior to upgrade (via saveJob + saveExecution)
        store.saveJob(job)
        store.saveExecution(execution)
        store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)

        assertFalse(store.seedInitialExecution(execution), "First seed after upgrade should skip existing completed row")
        assertTrue(store.cleanupCompletedExecutions(Instant.now().plusSeconds(3600)) >= 1)
        assertNull(store.getExecution(execution.id))

        assertFalse(
            store.seedInitialExecution(execution),
            "Legacy completed one-time job must remain marked as seeded even after retention cleanup"
        )
    }

    @Test
    fun `listExecutions filters by jobId, statuses, scheduled range boundaries, limit, and orders newest first`() = runTest {
        val store = createStore()
        val runId = UUID.randomUUID()
        val jobA = "query-job-a-$runId"
        val jobB = "query-job-b-$runId"
        val base = Instant.parse("2030-06-01T12:00:00Z")

        val (tieSmallerId, tieLargerId) = listOf(UUID.randomUUID(), UUID.randomUUID()).sortedBy { it.toString() }

        val execBeforeFrom = JobExecution(
            jobId = jobA,
            scheduledAt = base.minusMillis(1),
            status = ExecutionStatus.SUCCESS
        )
        val execAtFrom = JobExecution(
            jobId = jobA,
            scheduledAt = base,
            status = ExecutionStatus.FAILED
        )
        val execTieSmall = JobExecution(
            id = tieSmallerId,
            jobId = jobA,
            scheduledAt = base.plusSeconds(10),
            status = ExecutionStatus.PENDING
        )
        val execTieLarge = JobExecution(
            id = tieLargerId,
            jobId = jobA,
            scheduledAt = base.plusSeconds(10),
            status = ExecutionStatus.RUNNING
        )
        val execAtUntil = JobExecution(
            jobId = jobA,
            scheduledAt = base.plusSeconds(20),
            status = ExecutionStatus.CANCELLED
        )
        val execAfterUntil = JobExecution(
            jobId = jobA,
            scheduledAt = base.plusSeconds(20).plusMillis(1),
            status = ExecutionStatus.PENDING
        )
        val execOtherJob = JobExecution(
            jobId = jobB,
            scheduledAt = base.plusSeconds(15),
            status = ExecutionStatus.SUCCESS
        )

        listOf(
            execBeforeFrom,
            execAtFrom,
            execTieSmall,
            execTieLarge,
            execAtUntil,
            execAfterUntil,
            execOtherJob
        ).forEach { store.saveExecution(it) }

        // Full jobA listing ordered newest first, tie-broken by id DESC
        val allJobA = store.listExecutions(JobExecutionQuery(jobId = jobA))
        assertEquals(
            listOf(
                execAfterUntil.id,
                execAtUntil.id,
                execTieLarge.id,
                execTieSmall.id,
                execAtFrom.id,
                execBeforeFrom.id
            ),
            allJobA.map { it.id }
        )

        // Single status & multiple statuses filtering
        val onlyFailed = store.listExecutions(
            JobExecutionQuery(jobId = jobA, statuses = setOf(ExecutionStatus.FAILED))
        )
        assertEquals(listOf(execAtFrom.id), onlyFailed.map { it.id })

        val failedOrCancelled = store.listExecutions(
            JobExecutionQuery(jobId = jobA, statuses = setOf(ExecutionStatus.FAILED, ExecutionStatus.CANCELLED))
        )
        assertEquals(listOf(execAtUntil.id, execAtFrom.id), failedOrCancelled.map { it.id })

        // Empty status set returns empty list
        assertTrue(store.listExecutions(JobExecutionQuery(jobId = jobA, statuses = emptySet())).isEmpty())

        // Cross-job query (jobId = null)
        val allSuccess = store.listExecutions(JobExecutionQuery(statuses = setOf(ExecutionStatus.SUCCESS)))
        assertEquals(listOf(execOtherJob.id, execBeforeFrom.id), allSuccess.map { it.id })

        // Inclusive scheduledFrom and scheduledUntil boundaries
        val boundedRange = store.listExecutions(
            JobExecutionQuery(
                jobId = jobA,
                scheduledFrom = base,
                scheduledUntil = base.plusSeconds(20)
            )
        )
        assertEquals(
            listOf(execAtUntil.id, execTieLarge.id, execTieSmall.id, execAtFrom.id),
            boundedRange.map { it.id }
        )

        val fromOnly = store.listExecutions(JobExecutionQuery(jobId = jobA, scheduledFrom = base.plusSeconds(20)))
        assertEquals(listOf(execAfterUntil.id, execAtUntil.id), fromOnly.map { it.id })

        val untilOnly = store.listExecutions(JobExecutionQuery(jobId = jobA, scheduledUntil = base))
        assertEquals(listOf(execAtFrom.id, execBeforeFrom.id), untilOnly.map { it.id })

        // Bounded limit
        val topTwo = store.listExecutions(JobExecutionQuery(jobId = jobA, limit = 2))
        assertEquals(listOf(execAfterUntil.id, execAtUntil.id), topTwo.map { it.id })

        val topOne = store.listExecutions(JobExecutionQuery(jobId = jobA, limit = 1))
        assertEquals(listOf(execAfterUntil.id), topOne.map { it.id })
    }

    @Test
    fun `CANCELLED status is terminal sets completedAt releases locks and is eligible for retention cleanup`() = runTest {
        val store = createStore()
        val lockKey = "cancelled-lock-${UUID.randomUUID()}"
        val execution = JobExecution(
            jobId = "cancelled-job-${UUID.randomUUID()}",
            scheduledAt = Instant.now().minusSeconds(10),
            lockKey = lockKey
        )
        store.saveExecution(execution)

        assertTrue(store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5)))
        assertTrue(store.isLockHeld(lockKey))

        assertTrue(
            store.updateExecutionStatus(
                id = execution.id,
                status = ExecutionStatus.CANCELLED,
                error = "Stopped locally",
                workerId = "worker-1"
            )
        )

        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CANCELLED, updated?.status)
        assertNotNull(updated?.completedAt)
        assertFalse(store.isLockHeld(lockKey))
        assertFalse(store.listEligibleExecutions(Instant.now()).any { it.id == execution.id })

        val cleaned = store.cleanupCompletedExecutions(
            before = Instant.now().plusSeconds(60),
            statuses = setOf(ExecutionStatus.CANCELLED),
            limit = 100
        )
        assertEquals(1, cleaned)
        assertNull(store.getExecution(execution.id))
    }

    @Test
    fun `stores and retrieves JobDefinition paused flag`() = runTest {
        val store = createStore()
        val jobId = "paused-contract-${UUID.randomUUID()}"
        val activeJob = JobDefinition(
            id = jobId,
            handler = {},
            trigger = IntervalTrigger(Duration.ofMinutes(1)),
            paused = false
        )
        store.saveJob(activeJob)
        assertFalse(store.getJob(jobId)!!.paused)

        store.saveJob(activeJob.copy(paused = true))
        assertTrue(store.getJob(jobId)!!.paused)
    }

    @Test
    fun `claimExecution rejects paused jobs including expired claims and succeeds after resume`() = runTest {
        val store = createStore()
        val jobId = "paused-claim-contract-${UUID.randomUUID()}"
        val job = JobDefinition(
            id = jobId,
            handler = {},
            trigger = IntervalTrigger(Duration.ofMinutes(1)),
            paused = true
        )
        store.saveJob(job)

        val pendingExec = JobExecution(
            jobId = jobId,
            scheduledAt = Instant.now().minusSeconds(10),
            status = ExecutionStatus.PENDING
        )
        store.saveExecution(pendingExec)

        assertFalse(store.claimExecution(pendingExec.id, "worker-1", Duration.ofMinutes(1)))
        val stillPending = store.getExecution(pendingExec.id)
        assertEquals(ExecutionStatus.PENDING, stillPending?.status)
        assertNull(stillPending?.workerId)
        assertNull(stillPending?.expiresAt)

        store.saveJob(job.copy(paused = false))
        assertTrue(store.claimExecution(pendingExec.id, "worker-1", Duration.ofMinutes(1)))
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(pendingExec.id)?.status)

        val expiredExec = JobExecution(
            jobId = jobId,
            scheduledAt = Instant.now().minusSeconds(5),
            status = ExecutionStatus.PENDING
        )
        store.saveExecution(expiredExec)
        assertTrue(store.claimExecution(expiredExec.id, "worker-1", Duration.ofSeconds(-5)))

        store.saveJob(job.copy(paused = true))
        assertFalse(store.claimExecution(expiredExec.id, "worker-2", Duration.ofMinutes(1)))
        assertEquals("worker-1", store.getExecution(expiredExec.id)?.workerId)

        store.saveJob(job.copy(paused = false))
        assertTrue(store.claimExecution(expiredExec.id, "worker-2", Duration.ofMinutes(1)))
        assertEquals("worker-2", store.getExecution(expiredExec.id)?.workerId)

        assertFalse(store.claimExecution(UUID.randomUUID(), "worker-1", Duration.ofMinutes(1)))
    }

    @Test
    fun `checkHealth reports UP with non-negative latency on a healthy store`() = runTest {
        val store = createStore()
        val health = store.checkHealth()

        assertEquals(HealthStatus.UP, health.status)
        assertFalse(health.latency.isNegative)
        assertNull(health.errorType)
    }

    @Test
    fun `countExecutions counts all matching executions across filters without applying query limit`() = runTest {
        val store = createStore()
        val runId = UUID.randomUUID()
        val targetJob = "count-target-$runId"
        val otherJob = "count-other-$runId"
        val base = Instant.parse("2031-03-01T12:00:00Z")

        val dlq1 = JobExecution(jobId = targetJob, scheduledAt = base, status = ExecutionStatus.DEAD_LETTERED)
        val dlq2 = JobExecution(jobId = targetJob, scheduledAt = base.plusSeconds(10), status = ExecutionStatus.DEAD_LETTERED)
        val dlq3 = JobExecution(jobId = targetJob, scheduledAt = base.plusSeconds(20), status = ExecutionStatus.DEAD_LETTERED)
        val successSameJob = JobExecution(jobId = targetJob, scheduledAt = base.plusSeconds(30), status = ExecutionStatus.SUCCESS)
        val dlqOtherJob = JobExecution(jobId = otherJob, scheduledAt = base.plusSeconds(15), status = ExecutionStatus.DEAD_LETTERED)

        listOf(dlq1, dlq2, dlq3, successSameJob, dlqOtherJob).forEach { store.saveExecution(it) }

        assertEquals(
            3L,
            store.countExecutions(
                JobExecutionQuery(
                    jobId = targetJob,
                    statuses = setOf(ExecutionStatus.DEAD_LETTERED),
                    limit = 1
                )
            )
        )
        assertEquals(
            4L,
            store.countExecutions(JobExecutionQuery(jobId = targetJob, limit = 1))
        )
        assertEquals(
            4L,
            store.countExecutions(JobExecutionQuery(statuses = setOf(ExecutionStatus.DEAD_LETTERED), limit = 1))
        )
        assertEquals(
            0L,
            store.countExecutions(JobExecutionQuery(jobId = targetJob, statuses = emptySet()))
        )
        assertEquals(
            2L,
            store.countExecutions(
                JobExecutionQuery(
                    jobId = targetJob,
                    statuses = setOf(ExecutionStatus.DEAD_LETTERED),
                    scheduledFrom = base.plusSeconds(10),
                    scheduledUntil = base.plusSeconds(20)
                )
            )
        )
    }
}
