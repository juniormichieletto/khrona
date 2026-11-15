package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID

class JdbcLegacyFilteringTest {
    @Test
    fun `legacy filtered polling must find local job behind foreign backlog`() = runBlocking {
        HikariDataSource(HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:custom_${UUID.randomUUID()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"; password = ""
        }).use { ds ->
            val h2 = H2Dialect()
            val legacy = object : JdbcDialect {
                override fun upsertJobSql() = h2.upsertJobSql()
                override fun upsertExecutionSql() = h2.upsertExecutionSql()
                override fun claimExecutionSql() = h2.claimExecutionSql()
                override fun listEligibleExecutionsSql() = h2.listEligibleExecutionsSql()
                override fun heartbeatSql() = h2.heartbeatSql()
                override fun isLockHeldSql(excludeId: Boolean) = h2.isLockHeldSql(excludeId)
                override fun resetExpiredExecutionsSql() = h2.resetExpiredExecutionsSql()
                override fun cleanupCompletedExecutionsSql(statusCount: Int) = h2.cleanupCompletedExecutionsSql(statusCount)
            }
            val store = JdbcJobStore(ds, legacy)
            store.migrate()
            val job = JobDefinition(id = "custom-job", trigger = OneTimeTrigger(Instant.EPOCH))
            store.saveJob(job)
            repeat(7) { store.saveExecution(JobExecution(jobId = "foreign", scheduledAt = Instant.EPOCH)) }
            store.saveExecution(JobExecution(jobId = job.id, scheduledAt = Instant.EPOCH.plusSeconds(1)))
            val second = JobExecution(jobId = job.id, scheduledAt = Instant.EPOCH.plusSeconds(2))
            store.saveExecution(second)
            val filtered = store.listEligibleExecutions(Instant.now(), 1, setOf(job.id))
            assertEquals(1, filtered.size)
            assertEquals(job.id, filtered.single().jobId)
            assertEquals(2, store.listEligibleExecutions(Instant.now(), 2, setOf(job.id)).size)
            assertTrue(store.listEligibleExecutions(Instant.now(), 1, setOf("missing")).isEmpty())
            assertEquals("foreign", store.listEligibleExecutions(Instant.now(), 1, null).single().jobId)
        }
    }
}
