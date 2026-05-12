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
}
