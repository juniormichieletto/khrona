package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.time.Duration
import java.time.Instant
import java.util.*

class JdbcJobStoreTest {

    private lateinit var dataSource: HikariDataSource
    private lateinit var store: JdbcJobStore

    @BeforeEach
    fun setup() {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:khrona;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
            driverClassName = "org.h2.Driver"
        }
        dataSource = HikariDataSource(config)
        store = JdbcJobStore(dataSource)
        store.migrate()
    }

    @AfterEach
    fun tearDown() {
        dataSource.close()
    }

    @Test
    fun `should save and get job`() = runBlocking {
        val job = JobDefinition(
            id = "test-job",
            description = "Test description",
            handler = { },
            trigger = IntervalTrigger(Duration.ofMinutes(1)),
            retryPolicy = RetryPolicy(maxAttempts = 5)
        )
        
        store.saveJob(job)
        val saved = store.getJob("test-job")
        
        assertNotNull(saved)
        assertEquals(job.id, saved?.id)
        assertEquals(job.description, saved?.description)
        assertEquals(job.retryPolicy.maxAttempts, saved?.retryPolicy?.maxAttempts)
    }

    @Test
    fun `should save and claim execution`() = runBlocking {
        val job = JobDefinition(
            id = "test-job",
            handler = { },
            trigger = IntervalTrigger(Duration.ofMinutes(1))
        )
        store.saveJob(job)
        
        val execution = JobExecution(
            jobId = "test-job",
            scheduledAt = Instant.now().minusSeconds(60)
        )
        store.saveExecution(execution)
        
        val eligible = store.listEligibleExecutions(Instant.now())
        assertEquals(1, eligible.size)
        assertEquals(execution.id, eligible[0].id)
        
        val claimed = store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5))
        assertTrue(claimed)
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.CLAIMED, updated?.status)
        
        // Cannot claim twice
        val claimedAgain = store.claimExecution(execution.id, "worker-2", Duration.ofMinutes(5))
        assertFalse(claimedAgain)
    }

    @Test
    fun `should survive restart`() = runBlocking {
        val job = JobDefinition(id = "restart-job", handler = {}, trigger = IntervalTrigger(Duration.ZERO))
        store.saveJob(job)
        
        val execution = JobExecution(
            jobId = "restart-job",
            scheduledAt = Instant.now().minusSeconds(60)
        )
        store.saveExecution(execution)
        
        // "Restart" by closing and reopening datasource
        dataSource.close()
        
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:khrona;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
            username = "sa"
            password = ""
            driverClassName = "org.h2.Driver"
        }
        dataSource = HikariDataSource(config)
        store = JdbcJobStore(dataSource)
        
        val loaded = store.getExecution(execution.id)
        assertNotNull(loaded)
        assertEquals(execution.jobId, loaded?.jobId)
        assertEquals(ExecutionStatus.PENDING, loaded?.status)
    }

    @Test
    fun `should prevent double claiming by multiple workers`() = runBlocking {
        val job = JobDefinition(id = "race-job", handler = {}, trigger = IntervalTrigger(Duration.ZERO))
        store.saveJob(job)
        
        val execution = JobExecution(jobId = "race-job", scheduledAt = Instant.now())
        store.saveExecution(execution)
        
        val workers = 10
        val deferreds = (1..workers).map { i ->
            async(kotlinx.coroutines.Dispatchers.IO) {
                store.claimExecution(execution.id, "worker-$i", Duration.ofMinutes(5))
            }
        }
        val results = deferreds.awaitAll()
        
        assertEquals(1, results.count { it }, "Only one worker should succeed in claiming")
    }
}
