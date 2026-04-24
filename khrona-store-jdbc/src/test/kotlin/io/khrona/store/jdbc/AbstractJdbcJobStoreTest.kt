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
import javax.sql.DataSource

abstract class AbstractJdbcJobStoreTest {

    protected lateinit var dataSource: HikariDataSource
    protected lateinit var store: JdbcJobStore

    abstract fun createDataSource(): HikariDataSource

    @BeforeEach
    fun setup() {
        dataSource = createDataSource()
        store = JdbcJobStore(dataSource)
        store.migrate()
    }

    @AfterEach
    fun tearDown() {
        if (::dataSource.isInitialized) {
            dataSource.close()
        }
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
        val jdbcUrl = dataSource.jdbcUrl
        val username = dataSource.username
        val password = dataSource.password
        val driverClassName = dataSource.driverClassName
        
        dataSource.close()
        
        val config = HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            this.username = username
            this.password = password
            this.driverClassName = driverClassName
        }
        dataSource = HikariDataSource(config)
        store = JdbcJobStore(dataSource)
        
        val loaded = store.getExecution(execution.id)
        assertNotNull(loaded)
        assertEquals(execution.jobId, loaded?.jobId)
        assertEquals(ExecutionStatus.PENDING, loaded?.status)
    }

    @Test
    fun `should reclaim after lease expires`() = runBlocking {
        val job = JobDefinition(id = "lease-job", handler = {}, trigger = IntervalTrigger(Duration.ZERO))
        store.saveJob(job)
        
        val execution = JobExecution(jobId = "lease-job", scheduledAt = Instant.now().minusSeconds(60))
        store.saveExecution(execution)
        
        // Claim with 1ms lease
        store.claimExecution(execution.id, "worker-1", Duration.ofMillis(1))
        
        delay(10)
        
        val eligible = store.listEligibleExecutions(Instant.now())
        assertEquals(1, eligible.size)
        
        val claimedAgain = store.claimExecution(execution.id, "worker-2", Duration.ofMinutes(5))
        assertTrue(claimedAgain)
        
        val updated = store.getExecution(execution.id)
        assertEquals("worker-2", updated?.workerId)
    }

    @Test
    fun `should check if lock is held`() = runBlocking {
        val lockKey = "distributed-lock"
        val job = JobDefinition(id = "lock-job", handler = {}, trigger = IntervalTrigger(Duration.ZERO))
        store.saveJob(job)
        
        val execution = JobExecution(jobId = "lock-job", scheduledAt = Instant.now(), lockKey = lockKey)
        store.saveExecution(execution)
        
        assertFalse(store.isLockHeld(lockKey))
        
        store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5))
        assertTrue(store.isLockHeld(lockKey))
        
        store.updateExecutionStatus(execution.id, ExecutionStatus.SUCCESS)
        assertFalse(store.isLockHeld(lockKey))
    }

    @Test
    fun `should heartbeat to extend lease`() = runBlocking {
        val job = JobDefinition(id = "heartbeat-job", handler = {}, trigger = IntervalTrigger(Duration.ZERO))
        store.saveJob(job)
        
        val execution = JobExecution(jobId = "heartbeat-job", scheduledAt = Instant.now())
        store.saveExecution(execution)
        
        store.claimExecution(execution.id, "worker-1", Duration.ofMillis(100))
        val firstExpiry = store.getExecution(execution.id)?.expiresAt
        
        delay(10)
        store.heartbeat(execution.id, Duration.ofMinutes(1))
        val secondExpiry = store.getExecution(execution.id)?.expiresAt
        
        assertNotNull(firstExpiry)
        assertNotNull(secondExpiry)
        assertTrue(secondExpiry!! > firstExpiry!!)
    }
}
