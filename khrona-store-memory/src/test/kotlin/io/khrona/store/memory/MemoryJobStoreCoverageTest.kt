package io.khrona.store.memory

import io.khrona.core.ExecutionStatus
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
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

class MemoryJobStoreCoverageTest {
    @Test
    fun `lists saved jobs`() = runTest {
        val store = MemoryJobStore()
        val first = job("first")
        val second = job("second")

        store.saveJob(first)
        store.saveJob(second)

        assertEquals(setOf("first", "second"), store.listJobs().map { it.id }.toSet())
    }

    @Test
    fun `terminal statuses set completedAt while non terminal status preserves it`() = runTest {
        val store = MemoryJobStore()
        val execution = JobExecution(jobId = "status-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        store.updateExecutionStatus(execution.id, ExecutionStatus.RUNNING)
        assertNull(store.getExecution(execution.id)?.completedAt)

        listOf(
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.DEAD_LETTERED,
            ExecutionStatus.MISFIRED,
            ExecutionStatus.SUPERSEDED
        ).forEach { status ->
            val next = JobExecution(jobId = "status-$status", scheduledAt = Instant.now())
            store.saveExecution(next)

            store.updateExecutionStatus(next.id, status)

            assertEquals(status, store.getExecution(next.id)?.status)
            assertNotNull(store.getExecution(next.id)?.completedAt)
        }
    }

    @Test
    fun `eligible executions include expired active executions and exclude inactive ones`() = runTest {
        val store = MemoryJobStore()
        val now = Instant.parse("2030-01-01T00:00:00Z")
        val pendingDue = execution("pending-due", now.minusSeconds(1))
        val future = execution("future", now.plusSeconds(1))
        val expiredClaimed = execution(
            "expired-claimed",
            now.minusSeconds(1),
            status = ExecutionStatus.CLAIMED,
            expiresAt = now.minusMillis(1)
        )
        val expiredRunning = execution(
            "expired-running",
            now.minusSeconds(1),
            status = ExecutionStatus.RUNNING,
            expiresAt = now.minusMillis(1)
        )
        val activeClaimed = execution(
            "active-claimed",
            now.minusSeconds(1),
            status = ExecutionStatus.CLAIMED,
            expiresAt = now.plusSeconds(1)
        )
        val claimedWithoutExpiry = execution(
            "claimed-without-expiry",
            now.minusSeconds(1),
            status = ExecutionStatus.CLAIMED
        )
        val success = execution("success", now.minusSeconds(1), status = ExecutionStatus.SUCCESS)

        listOf(pendingDue, future, expiredClaimed, expiredRunning, activeClaimed, claimedWithoutExpiry, success)
            .forEach { store.saveExecution(it) }

        val eligible = store.listEligibleExecutions(now).map { it.id }.toSet()

        assertEquals(setOf(pendingDue.id, expiredClaimed.id, expiredRunning.id), eligible)
    }

    @Test
    fun `claim heartbeat and lock checks return false for missing or inactive executions`() = runTest {
        val store = MemoryJobStore()
        val now = Instant.now()
        val success = execution("success", now, status = ExecutionStatus.SUCCESS, lockKey = "lock")
        val running = execution("running", now, status = ExecutionStatus.RUNNING, lockKey = "lock")
        store.saveExecution(success)
        store.saveExecution(running)

        assertFalse(store.claimExecution(UUID.randomUUID(), "worker", Duration.ofMinutes(1)))
        assertFalse(store.claimExecution(success.id, "worker", Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(UUID.randomUUID(), Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(success.id, Duration.ofMinutes(1)))

        assertTrue(store.heartbeat(running.id, Duration.ofMinutes(1)))
        assertTrue(store.isLockHeld("lock"))
        assertFalse(store.isLockHeld("lock", excludeExecutionId = running.id))
        assertFalse(store.isLockHeld("missing-lock"))

        // Coverage for expired running execution claim
        val expiredRunning = execution("expired-running-claim", now, status = ExecutionStatus.RUNNING, expiresAt = now.minusSeconds(1))
        store.saveExecution(expiredRunning)
        assertTrue(store.claimExecution(expiredRunning.id, "new-worker", Duration.ofMinutes(1)))

        // Attempting to claim active claimed or running execution
        val activeClaimed = execution("active-claimed", now, status = ExecutionStatus.CLAIMED, expiresAt = now.plusSeconds(300))
        val activeRunning = execution("active-running", now, status = ExecutionStatus.RUNNING, expiresAt = now.plusSeconds(300))
        val claimedNullExpiry = execution("claimed-null-expiry", now, status = ExecutionStatus.CLAIMED, expiresAt = null)
        store.saveExecution(activeClaimed)
        store.saveExecution(activeRunning)
        store.saveExecution(claimedNullExpiry)
        assertFalse(store.claimExecution(activeClaimed.id, "other-worker", Duration.ofMinutes(1)))
        assertFalse(store.claimExecution(activeRunning.id, "other-worker", Duration.ofMinutes(1)))
        assertFalse(store.claimExecution(claimedNullExpiry.id, "other-worker", Duration.ofMinutes(1)))

        // Coverage for isLockHeld branches (CLAIMED without expiry, RUNNING without expiry, expired locks)
        val claimedNoExpiry = execution("claimed-no-exp", now, status = ExecutionStatus.CLAIMED, expiresAt = null, lockKey = "lock-no-exp-claimed")
        val runningNoExpiry = execution("running-no-exp", now, status = ExecutionStatus.RUNNING, expiresAt = null, lockKey = "lock-no-exp-running")
        val claimedExpiredLock = execution("claimed-exp-lock", now, status = ExecutionStatus.CLAIMED, expiresAt = now.minusSeconds(1), lockKey = "lock-exp-claimed")
        val runningExpiredLock = execution("running-exp-lock", now, status = ExecutionStatus.RUNNING, expiresAt = now.minusSeconds(1), lockKey = "lock-exp-running")
        store.saveExecution(claimedNoExpiry)
        store.saveExecution(runningNoExpiry)
        store.saveExecution(claimedExpiredLock)
        store.saveExecution(runningExpiredLock)

        assertTrue(store.isLockHeld("lock-no-exp-claimed"))
        assertTrue(store.isLockHeld("lock-no-exp-running"))
        assertFalse(store.isLockHeld("lock-exp-claimed"))
        assertFalse(store.isLockHeld("lock-exp-running"))
    }

    @Test
    fun `reset expired executions only resets expired claimed or running executions`() = runTest {
        val store = MemoryJobStore()
        val now = Instant.parse("2030-01-01T00:00:00Z")
        val expiredClaimed = execution("expired-claimed", now, ExecutionStatus.CLAIMED, expiresAt = now.minusSeconds(1))
        val expiredRunning = execution("expired-running", now, ExecutionStatus.RUNNING, expiresAt = now.minusSeconds(1))
        val activeClaimed = execution("active-claimed", now, ExecutionStatus.CLAIMED, expiresAt = now.plusSeconds(1))
        val claimedWithoutExpiry = execution("claimed-without-expiry", now, ExecutionStatus.CLAIMED)
        val pending = execution("pending", now)
        listOf(expiredClaimed, expiredRunning, activeClaimed, claimedWithoutExpiry, pending).forEach {
            store.saveExecution(it.copy(workerId = "worker", startedAt = now.minusSeconds(10)))
        }

        assertEquals(2, store.resetExpiredExecutions(now))

        listOf(expiredClaimed, expiredRunning).forEach {
            val reset = store.getExecution(it.id)
            assertEquals(ExecutionStatus.PENDING, reset?.status)
            assertNull(reset?.workerId)
            assertNull(reset?.startedAt)
            assertNull(reset?.expiresAt)
        }
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(activeClaimed.id)?.status)
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(claimedWithoutExpiry.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pending.id)?.status)
    }

    @Test
    fun `supersede marks only active matching lock executions except excluded execution`() = runTest {
        val store = MemoryJobStore()
        val lockKey = "replace-lock"
        val claimed = execution("claimed", Instant.now(), ExecutionStatus.CLAIMED, lockKey = lockKey)
        val running = execution("running", Instant.now(), ExecutionStatus.RUNNING, lockKey = lockKey)
        val excluded = execution("excluded", Instant.now(), ExecutionStatus.RUNNING, lockKey = lockKey)
        val pending = execution("pending", Instant.now(), ExecutionStatus.PENDING, lockKey = lockKey)
        val otherLock = execution("other", Instant.now(), ExecutionStatus.RUNNING, lockKey = "other-lock")
        listOf(claimed, running, excluded, pending, otherLock).forEach { store.saveExecution(it) }

        val superseded = store.supersedeExecutionsByLockKey(lockKey, excludeExecutionId = excluded.id)

        assertEquals(setOf(claimed.id, running.id), superseded.toSet())
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(claimed.id)?.status)
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(running.id)?.status)
        assertNotNull(store.getExecution(claimed.id)?.completedAt)
        assertEquals(ExecutionStatus.RUNNING, store.getExecution(excluded.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pending.id)?.status)
        assertEquals(ExecutionStatus.RUNNING, store.getExecution(otherLock.id)?.status)
    }

    private fun job(id: String) = JobDefinition(
        id = id,
        handler = {},
        trigger = IntervalTrigger(Duration.ofMinutes(1))
    )

    private fun execution(
        jobId: String,
        scheduledAt: Instant,
        status: ExecutionStatus = ExecutionStatus.PENDING,
        expiresAt: Instant? = null,
        lockKey: String? = null
    ) = JobExecution(
        jobId = jobId,
        scheduledAt = scheduledAt,
        status = status,
        expiresAt = expiresAt,
        lockKey = lockKey
    )
}
