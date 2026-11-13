package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobExecution
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant

@Testcontainers
class MySqlJobStoreTest : AbstractJdbcJobStoreTest() {

    companion object {
        @Container
        val mysql = MySQLContainer("mysql:8.0").apply {
            withDatabaseName("khrona")
            withUsername("test")
            withPassword("test")
        }
    }

    override fun createDataSource(): HikariDataSource {
        val config = HikariConfig().apply {
            jdbcUrl = mysql.jdbcUrl
            username = mysql.username
            password = mysql.password
            driverClassName = mysql.driverClassName
        }
        return HikariDataSource(config)
    }

    @Test
    fun `cleanupCompletedExecutions skips locked rows on MySQL`() = runBlocking {
        val oldTime = Instant.now().minusSeconds(3600)
        val exec1 = JobExecution(
            jobId = "mysql-skip-locked-1",
            scheduledAt = oldTime.minusSeconds(10),
            status = ExecutionStatus.SUCCESS,
            completedAt = oldTime
        )
        val exec2 = JobExecution(
            jobId = "mysql-skip-locked-2",
            scheduledAt = oldTime.minusSeconds(5),
            status = ExecutionStatus.SUCCESS,
            completedAt = oldTime
        )
        store.saveExecution(exec1)
        store.saveExecution(exec2)
        store.updateExecutionStatus(exec1.id, ExecutionStatus.SUCCESS)
        store.updateExecutionStatus(exec2.id, ExecutionStatus.SUCCESS)

        dataSource.connection.use { lockConn ->
            lockConn.autoCommit = false
            lockConn.prepareStatement("SELECT id FROM khrona_executions WHERE id = ? FOR UPDATE").use { stmt ->
                stmt.setString(1, exec1.id.toString())
                stmt.executeQuery().use { it.next() }
            }

            val cleaned = store.cleanupCompletedExecutions(
                before = Instant.now().plusSeconds(60),
                statuses = setOf(ExecutionStatus.SUCCESS),
                limit = 1
            )
            assertEquals(1, cleaned)
            lockConn.rollback()
        }

        assertNotNull(store.getExecution(exec1.id))
        assertNull(store.getExecution(exec2.id))
    }
}
