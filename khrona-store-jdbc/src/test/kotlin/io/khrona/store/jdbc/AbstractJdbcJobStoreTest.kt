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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

abstract class AbstractJdbcJobStoreTest {

    protected lateinit var dataSource: HikariDataSource
    protected lateinit var store: JdbcJobStore
    private val log = org.slf4j.LoggerFactory.getLogger(AbstractJdbcJobStoreTest::class.java)

    abstract fun createDataSource(): HikariDataSource

    @BeforeEach
    fun setup() {
        dataSource = createDataSource()
        store = JdbcJobStore(dataSource)
        runBlocking { store.migrate() }
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
        assertTrue(saved?.trigger is IntervalTrigger)
    }

    @Test
    fun `should save and get job with cron trigger`() = runBlocking {
        val job = JobDefinition(
            id = "cron-job",
            handler = { },
            trigger = CronTrigger("0 * * * *")
        )
        
        store.saveJob(job)
        val saved = store.getJob("cron-job")
        
        assertNotNull(saved)
        assertTrue(saved?.trigger is CronTrigger)
        assertEquals("0 * * * *", (saved?.trigger as CronTrigger).expression)
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
        
        // Claim with an already expired lease (negative duration)
        store.claimExecution(execution.id, "worker-1", Duration.ofSeconds(-10))
        
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

    @Test
    fun `should ensure only one instance executes a specific job across multiple instances`() = runBlocking {
        val instanceCount = 6
        val executionCount = 10
        val jobId = "highly-contested-job-${UUID.randomUUID()}"
        val lockKey = "shared-lock-${UUID.randomUUID()}"

        val executionLog = ConcurrentHashMap<String, AtomicInteger>()
        val schedulers = (1..instanceCount).map { instance ->
            val config = KhronaConfig().apply {
                this.store = this@AbstractJdbcJobStoreTest.store
                pollingInterval = Duration.ofMillis(100)
            }
            Scheduler(config, CoroutineScope(Dispatchers.Default + SupervisorJob())).also { scheduler ->
                scheduler.registerJob(
                    JobDefinition(
                        id = jobId,
                        trigger = IntervalTrigger(Duration.ofMinutes(1)),
                        concurrencyPolicy = ConcurrencyPolicy.FORBID,
                        lockKey = lockKey,
                        handler = { payload ->
                            val serial = payload as String
                            log.info("Worker $instance executing $serial")
                            executionLog.computeIfAbsent(serial) { AtomicInteger(0) }.incrementAndGet()
                            delay(200)
                        }
                    )
                )
                scheduler.start()
            }
        }

        try {
            repeat(executionCount) { index ->
                val serial = "exec-$index"
                store.saveExecution(
                    JobExecution(
                        jobId = jobId,
                        scheduledAt = Instant.now(),
                        payload = serial,
                        lockKey = lockKey
                    )
                )
            }

            withTimeout(15_000) {
                while (executionLog.size < executionCount) {
                    delay(500)
                    log.info("Progress: ${executionLog.size}/$executionCount processed")
                }
            }

            executionLog.forEach { (serial, count) ->
                assertEquals(1, count.get(), "Execution $serial should have been processed exactly once across all instances")
            }
        } finally {
            schedulers.forEach { it.stop() }
        }
    }

    @Test
    fun `should allow parallel execution of different jobs across instances`() = runBlocking {
        val instanceCount = 4
        val executionLog = ConcurrentHashMap<String, Long>()
        val testRunId = UUID.randomUUID()

        val schedulers = (1..instanceCount).map { index ->
            val jobId = "parallel-job-$testRunId-$index"
            val config = KhronaConfig().apply {
                this.store = this@AbstractJdbcJobStoreTest.store
                pollingInterval = Duration.ofMillis(100)
            }
            Scheduler(config, CoroutineScope(Dispatchers.Default + SupervisorJob())).also { scheduler ->
                scheduler.registerJob(
                    JobDefinition(
                        id = jobId,
                        trigger = IntervalTrigger(Duration.ofHours(1)),
                        handler = {
                            log.info("Executing $jobId")
                            executionLog[jobId] = System.currentTimeMillis()
                            delay(1_000)
                        }
                    )
                )
                scheduler.start()
            }
        }

        try {
            repeat(instanceCount) { index ->
                val jobId = "parallel-job-$testRunId-${index + 1}"
                store.saveExecution(JobExecution(jobId = jobId, scheduledAt = Instant.now()))
            }

            withTimeout(10_000) {
                while (executionLog.size < instanceCount) {
                    delay(500)
                    log.info("Parallel progress: ${executionLog.size}/$instanceCount processed")
                }
            }

            val times = executionLog.values.sorted()
            val delta = times.last() - times.first()
            assertTrue(delta < 2_000, "Jobs should have started in parallel, but delta was ${delta}ms")
        } finally {
            schedulers.forEach { it.stop() }
        }
    }
}
