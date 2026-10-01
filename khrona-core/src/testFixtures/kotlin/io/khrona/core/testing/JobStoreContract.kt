package io.khrona.core.testing

import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobStore
import io.khrona.core.IntervalTrigger
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
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

        store.claimExecution(execution.id, "worker-1", Duration.ofMillis(100))
        val firstExpiry = store.getExecution(execution.id)?.expiresAt

        val heartbeated = store.heartbeat(execution.id, Duration.ofMinutes(1))
        val secondExpiry = store.getExecution(execution.id)?.expiresAt

        assertTrue(heartbeated)
        assertNotNull(firstExpiry)
        assertNotNull(secondExpiry)
        assertTrue(secondExpiry!! > firstExpiry!!)
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
}
