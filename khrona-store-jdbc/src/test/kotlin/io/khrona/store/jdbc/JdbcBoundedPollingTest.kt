package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class JdbcBoundedPollingTest {

    private lateinit var dataSource: HikariDataSource
    private lateinit var store: JdbcJobStore

    @BeforeEach
    fun setup() {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:bounded_test;DB_CLOSE_DELAY=-1"
            driverClassName = "org.h2.Driver"
        }
        dataSource = HikariDataSource(config)
        store = JdbcJobStore(dataSource)
        runBlocking { store.migrate() }
    }

    @AfterEach
    fun tearDown() {
        dataSource.close()
    }

    @Test
    fun `should only return up to limit eligible executions`() = runBlocking {
        val job = JobDefinition(
            id = "test-job",
            handler = { },
            trigger = IntervalTrigger(Duration.ofMinutes(1))
        )
        store.saveJob(job)
        
        // Create 10 eligible executions
        repeat(10) { i ->
            store.saveExecution(JobExecution(
                jobId = "test-job",
                scheduledAt = Instant.now().minusSeconds(10 + i.toLong())
            ))
        }
        
        val eligible = store.listEligibleExecutions(Instant.now(), limit = 3)
        assertEquals(3, eligible.size)
        
        val allEligible = store.listEligibleExecutions(Instant.now(), limit = 20)
        assertEquals(10, allEligible.size)
    }
}
