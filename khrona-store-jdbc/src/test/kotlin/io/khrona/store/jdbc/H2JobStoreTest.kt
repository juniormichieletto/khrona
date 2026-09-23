package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class H2JobStoreTest : AbstractJdbcJobStoreTest() {
    override fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:khrona_${System.currentTimeMillis()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
            driverClassName = "org.h2.Driver"
        }
        return HikariDataSource(config)
    }

    @Test
    fun `H2 store lists jobs and returns null for missing records`() = runBlocking {
        val first = job("h2-list-first")
        val second = job("h2-list-second")

        store.saveJob(first)
        store.saveJob(second)

        assertEquals(setOf(first.id, second.id), store.listJobs().map { it.id }.toSet())
        assertNull(store.getJob("missing-job"))
        assertNull(store.getExecution(java.util.UUID.randomUUID()))
    }

    @Test
    fun `H2 store covers terminal status and inactive claim branches`() = runBlocking {
        val running = JobExecution(jobId = "h2-running", scheduledAt = Instant.now())
        val success = JobExecution(jobId = "h2-success", scheduledAt = Instant.now())
        store.saveExecution(running)
        store.saveExecution(success)

        store.updateExecutionStatus(running.id, ExecutionStatus.RUNNING)
        store.updateExecutionStatus(success.id, ExecutionStatus.SUCCESS, "done")

        assertNull(store.getExecution(running.id)?.completedAt)
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(success.id)?.status)
        assertEquals("done", store.getExecution(success.id)?.error)
        assertNotNull(store.getExecution(success.id)?.completedAt)
        assertFalse(store.claimExecution(success.id, "worker", Duration.ofMinutes(1)))
        assertFalse(store.claimExecution(java.util.UUID.randomUUID(), "worker", Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(success.id, Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(java.util.UUID.randomUUID(), Duration.ofMinutes(1)))
    }

    @Test
    fun `H2 store covers eligible reset lock exclusion and supersede branches`() = runBlocking {
        val now = Instant.now()
        val lockKey = "h2-replace-lock"
        val expiredClaimed = JobExecution(
            jobId = "h2-expired-claimed",
            scheduledAt = now.minusSeconds(10),
            lockKey = lockKey
        )
        val expiredRunning = JobExecution(
            jobId = "h2-expired-running",
            scheduledAt = now.minusSeconds(9),
            lockKey = lockKey
        )
        val activeClaimed = JobExecution(jobId = "h2-active-claimed", scheduledAt = now, lockKey = lockKey)
        val activeRunning = JobExecution(jobId = "h2-active-running", scheduledAt = now, lockKey = lockKey)
        val excluded = JobExecution(jobId = "h2-excluded", scheduledAt = now, lockKey = lockKey)
        val pending = JobExecution(jobId = "h2-pending", scheduledAt = now, lockKey = lockKey)
        val otherLock = JobExecution(jobId = "h2-other-lock", scheduledAt = now, lockKey = "other-lock")
        listOf(expiredClaimed, expiredRunning, activeClaimed, activeRunning, excluded, pending, otherLock)
            .forEach { store.saveExecution(it) }
        assertTrue(store.claimExecution(expiredClaimed.id, "old-worker", Duration.ofSeconds(-1)))
        assertTrue(store.claimExecution(expiredRunning.id, "old-worker", Duration.ofSeconds(-1)))
        store.updateExecutionStatus(expiredRunning.id, ExecutionStatus.RUNNING)

        val eligibleBeforeReset = store.listEligibleExecutions(now, 10).map { it.id }.toSet()
        assertTrue(eligibleBeforeReset.contains(expiredClaimed.id))
        assertTrue(eligibleBeforeReset.contains(expiredRunning.id))
        assertEquals(2, store.resetExpiredExecutions(now))
        assertTrue(store.claimExecution(activeClaimed.id, "worker-1", Duration.ofMinutes(5)))
        assertTrue(store.claimExecution(activeRunning.id, "worker-2", Duration.ofMinutes(5)))
        store.updateExecutionStatus(activeRunning.id, ExecutionStatus.RUNNING)
        assertTrue(store.claimExecution(excluded.id, "worker-3", Duration.ofMinutes(5)))

        assertTrue(store.isLockHeld(lockKey))
        assertTrue(store.isLockHeld(lockKey, excludeExecutionId = excluded.id))
        assertFalse(store.isLockHeld("missing-lock"))

        val superseded = store.supersedeExecutionsByLockKey(lockKey, excludeExecutionId = excluded.id)

        assertEquals(setOf(activeClaimed.id, activeRunning.id), superseded.toSet())
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(activeClaimed.id)?.status)
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(activeRunning.id)?.status)
        assertNotNull(store.getExecution(activeClaimed.id)?.completedAt)
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(excluded.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pending.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(otherLock.id)?.status)
        assertEquals(emptyList<java.util.UUID>(), store.supersedeExecutionsByLockKey("missing-lock"))
    }

    private fun job(id: String) = JobDefinition(
        id = id,
        handler = {},
        trigger = IntervalTrigger(Duration.ofMinutes(1))
    )
}
