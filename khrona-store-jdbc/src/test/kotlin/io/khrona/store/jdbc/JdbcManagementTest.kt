package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobExecutionQuery
import io.khrona.core.JobStore
import io.khrona.core.KhronaConfig
import io.khrona.core.MisfirePolicy
import io.khrona.core.Scheduler
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class JdbcManagementTest {

    private lateinit var dataSource: HikariDataSource
    private lateinit var store: JdbcJobStore

    @BeforeEach
    fun setUp() = runBlocking {
        dataSource = HikariDataSource(
            HikariConfig().apply {
                jdbcUrl = "jdbc:h2:mem:khrona_mgmt_${UUID.randomUUID()};MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1"
                username = "sa"
                password = ""
                driverClassName = "org.h2.Driver"
            }
        )
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
    fun `shutdown checkpoint releases execution in H2 so restarted scheduler runs immediately without lease expiry`() = runBlocking {
        val firstEntered = CompletableDeferred<Unit>()
        val proceedToCheckpoint = CompletableDeferred<Unit>()
        val firstCompletedAfterCheckpoint = AtomicBoolean(false)
        val scope1 = CoroutineScope(Dispatchers.Default + SupervisorJob())

        val scheduler1 = Scheduler(
            KhronaConfig().apply {
                this.store = this@JdbcManagementTest.store
                pollingInterval = Duration.ofMillis(50)
                executionLeaseDuration = Duration.ofMinutes(5)
                heartbeatInterval = Duration.ofMinutes(1)
                shutdownTimeout = Duration.ofSeconds(10)
                job("durable-checkpoint-job") {
                    once()
                    execute {
                        if (!firstEntered.isCompleted) {
                            firstEntered.complete(Unit)
                            proceedToCheckpoint.await()
                            checkpoint()
                            firstCompletedAfterCheckpoint.set(true)
                        }
                    }
                }
            },
            scope1
        )

        scheduler1.start()
        withTimeout(5_000) { firstEntered.await() }

        val stopTask = async { scheduler1.stop() }
        delay(50)
        proceedToCheckpoint.complete(Unit)
        withTimeout(5_000) { stopTask.await() }

        assertFalse(firstCompletedAfterCheckpoint.get())

        val historyAfterStop = store.listExecutions(
            io.khrona.core.JobExecutionQuery(jobId = "durable-checkpoint-job")
        )
        assertEquals(1, historyAfterStop.size)
        val released = historyAfterStop.single()
        assertEquals(ExecutionStatus.PENDING, released.status)
        assertNull(released.workerId)
        assertNull(released.expiresAt)
        assertNull(released.completedAt)

        // Boot a second scheduler immediately (5-minute lease would NOT have expired)
        val secondRan = CompletableDeferred<UUID>()
        val scope2 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scheduler2 = Scheduler(
            KhronaConfig().apply {
                this.store = JdbcJobStore(dataSource)
                pollingInterval = Duration.ofMillis(50)
                executionLeaseDuration = Duration.ofMinutes(5)
                heartbeatInterval = Duration.ofMinutes(1)
                shutdownTimeout = Duration.ofSeconds(2)
                job("durable-checkpoint-job") {
                    once()
                    execute {
                        checkpoint()
                        secondRan.complete(executionId)
                    }
                }
            },
            scope2
        )

        scheduler2.start()
        try {
            val ranId = withTimeout(5_000) { secondRan.await() }
            assertEquals(released.id, ranId)
            withTimeout(5_000) {
                while (store.getExecution(ranId)?.status != ExecutionStatus.SUCCESS) {
                    delay(20)
                }
            }
        } finally {
            scheduler2.stop()
        }
    }

    @Test
    fun `pause resume management facade and local stop work end to end against H2 JdbcJobStore`() = runBlocking {
        val runs = AtomicInteger(0)
        val activeEntered = CompletableDeferred<UUID>()
        val activeCancelled = CompletableDeferred<Boolean>()

        val scope1 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scope2 = CoroutineScope(Dispatchers.Default + SupervisorJob())

        val scheduler1 = Scheduler(
            KhronaConfig().apply {
                this.store = this@JdbcManagementTest.store
                pollingInterval = Duration.ofMillis(50)
                shutdownTimeout = Duration.ofSeconds(2)
                job("jdbc-managed-job") {
                    every(Duration.ofHours(1))
                    execute {
                        runs.incrementAndGet()
                    }
                }
                job("jdbc-stoppable-job") {
                    once()
                    execute {
                        activeEntered.complete(executionId)
                        try {
                            awaitCancellation()
                        } catch (e: CancellationException) {
                            activeCancelled.complete(true)
                            throw e
                        }
                    }
                }
            },
            scope1
        )

        val scheduler2 = Scheduler(
            KhronaConfig().apply {
                this.store = JdbcJobStore(dataSource)
                pollingInterval = Duration.ofMillis(50)
                shutdownTimeout = Duration.ofSeconds(2)
                job("jdbc-managed-job") {
                    every(Duration.ofHours(1))
                    execute {
                        runs.incrementAndGet()
                    }
                }
            },
            scope2
        )

        try {
            scheduler1.config.jobs.forEach { store.saveJob(it) }
            assertTrue(scheduler1.pauseJob("jdbc-managed-job").success)

            // Queue manual work while paused; scheduler2 starts and must respect paused=true
            val queued = scheduler1.startJob("jdbc-managed-job", payload = mapOf("step" to "one"))
            assertTrue(queued.success)
            val queuedId = queued.affectedExecutionIds.single()

            scheduler2.start()
            delay(250)
            assertEquals(0, runs.get())
            assertEquals(ExecutionStatus.PENDING, store.getExecution(queuedId)?.status)
            assertTrue(scheduler2.getJobOverview("jdbc-managed-job")!!.paused)

            assertTrue(scheduler1.resumeJob("jdbc-managed-job").success)
            withTimeout(5_000) {
                while (store.getExecution(queuedId)?.status != ExecutionStatus.SUCCESS) {
                    delay(20)
                }
            }
            assertTrue(runs.get() >= 1)

            val overviews = scheduler1.listJobOverviews()
            assertEquals(listOf("jdbc-managed-job", "jdbc-stoppable-job"), overviews.map { it.job.id })
            assertEquals(1, scheduler1.listJobHistory("jdbc-managed-job", limit = 1).size)

            // Local stop on scheduler1
            scheduler1.start()
            val stopTargetId = scheduler1.startJob("jdbc-stoppable-job").affectedExecutionIds.single()
            val startedId = withTimeout(5_000) { activeEntered.await() }
            assertEquals(stopTargetId, startedId)

            val stopResult = scheduler1.stopExecution(stopTargetId)
            assertTrue(stopResult.success)
            assertTrue(withTimeout(5_000) { activeCancelled.await() })
            val cancelledExec = store.getExecution(stopTargetId)
            assertEquals(ExecutionStatus.CANCELLED, cancelledExec?.status)
            assertNotNull(cancelledExec?.completedAt)
        } finally {
            scheduler1.stop()
            scheduler2.stop()
        }
    }

    @Test
    fun `poll racing with pauseJob does not claim execute or misfire against H2 JdbcJobStore`() = runBlocking {
        val runs = AtomicInteger(0)
        val inPoll = AtomicBoolean(false)
        val normalPauseTriggered = CompletableDeferred<Unit>()
        val ignorePauseTriggered = CompletableDeferred<Unit>()
        val scope1 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val scope2 = CoroutineScope(Dispatchers.Default + SupervisorJob())
        lateinit var scheduler2: Scheduler

        val interceptingStore = object : JobStore by store {
            override suspend fun listEligibleExecutions(
                now: Instant,
                limit: Int,
                jobIds: Set<String>?
            ): List<JobExecution> {
                return store.listEligibleExecutions(now, limit, jobIds).also {
                    if (it.isNotEmpty()) {
                        inPoll.set(true)
                    }
                }
            }

            override suspend fun getJob(jobId: String): JobDefinition? {
                val jobDef = store.getJob(jobId)
                if (inPoll.get() && jobDef != null && !jobDef.paused) {
                    if (jobId == "h2-race-normal" && !normalPauseTriggered.isCompleted) {
                        assertTrue(scheduler2.pauseJob(jobId).success)
                        normalPauseTriggered.complete(Unit)
                    } else if (jobId == "h2-race-ignore" && !ignorePauseTriggered.isCompleted) {
                        assertTrue(scheduler2.pauseJob(jobId).success)
                        ignorePauseTriggered.complete(Unit)
                    }
                }
                return jobDef
            }
        }

        val scheduler1 = Scheduler(
            KhronaConfig().apply {
                this.store = interceptingStore
                pollingInterval = Duration.ofMillis(50)
                misfireThreshold = Duration.ofSeconds(5)
                shutdownTimeout = Duration.ofSeconds(2)
                job("h2-race-normal") {
                    every(Duration.ofHours(1))
                    misfirePolicy = MisfirePolicy.FIRE_NOW
                    execute { runs.incrementAndGet() }
                }
                job("h2-race-ignore") {
                    every(Duration.ofHours(1))
                    misfirePolicy = MisfirePolicy.IGNORE
                    execute { runs.incrementAndGet() }
                }
            },
            scope1
        )
        scheduler2 = Scheduler(
            KhronaConfig().apply {
                this.store = JdbcJobStore(dataSource)
                pollingInterval = Duration.ofHours(1)
                shutdownTimeout = Duration.ofSeconds(1)
            },
            scope2
        )

        try {
            scheduler1.config.jobs.forEach { store.saveJob(it) }
            val overdueTime = Instant.now().minusSeconds(60)
            val normalExec = JobExecution(jobId = "h2-race-normal", scheduledAt = overdueTime)
            val ignoreExec = JobExecution(jobId = "h2-race-ignore", scheduledAt = overdueTime)
            store.saveExecution(normalExec)
            store.saveExecution(ignoreExec)

            scheduler1.start()
            withTimeout(5_000) {
                normalPauseTriggered.await()
                ignorePauseTriggered.await()
            }
            delay(250)

            assertEquals(0, runs.get())
            assertEquals(ExecutionStatus.PENDING, store.getExecution(normalExec.id)?.status)
            assertNull(store.getExecution(normalExec.id)?.workerId)
            assertEquals(ExecutionStatus.PENDING, store.getExecution(ignoreExec.id)?.status)
            assertNull(store.getExecution(ignoreExec.id)?.workerId)
            assertEquals(1, store.listExecutions(JobExecutionQuery(jobId = "h2-race-ignore")).size)
        } finally {
            scheduler1.stop()
            scheduler2.stop()
        }
    }

    @Test
    fun `claimExecution serializes with uncommitted pauseJob transaction on khrona_jobs`() = runBlocking {
        val jobId = "h2-lock-pause-${UUID.randomUUID()}"
        val scheduler = Scheduler(
            KhronaConfig().apply {
                this.store = this@JdbcManagementTest.store
                job(jobId) {
                    once()
                    execute {}
                }
            },
            CoroutineScope(Dispatchers.Default + SupervisorJob())
        )
        val jobDef = scheduler.config.jobs.single()
        store.saveJob(jobDef)

        val execution = JobExecution(jobId = jobId, scheduledAt = Instant.now().minusSeconds(5))
        store.saveExecution(execution)

        val txConn = dataSource.connection
        var txOpen = true
        try {
            txConn.autoCommit = false
            val currentJson = txConn.prepareStatement("SELECT definition_json FROM khrona_jobs WHERE id = ? FOR UPDATE").use { stmt ->
                stmt.setString(1, jobId)
                stmt.executeQuery().use { rs ->
                    assertTrue(rs.next())
                    rs.getString("definition_json")!!
                }
            }
            val parsedObj = Json.parseToJsonElement(currentJson) as JsonObject
            val pausedJson = Json.encodeToString(
                JsonObject.serializer(),
                JsonObject(parsedObj + ("paused" to JsonPrimitive(true)))
            )
            txConn.prepareStatement("UPDATE khrona_jobs SET definition_json = ? WHERE id = ?").use { stmt ->
                stmt.setString(1, pausedJson)
                stmt.setString(2, jobId)
                assertEquals(1, stmt.executeUpdate())
            }

            val claimDeferred = async(Dispatchers.Default) {
                store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(1))
            }
            delay(100)
            assertFalse(claimDeferred.isCompleted, "claimExecution must block while khrona_jobs row is locked FOR UPDATE")

            txConn.commit()
            txOpen = false

            assertFalse(withTimeout(5_000) { claimDeferred.await() })
            assertEquals(ExecutionStatus.PENDING, store.getExecution(execution.id)?.status)
        } finally {
            if (txOpen) {
                runCatching { txConn.rollback() }
            }
            runCatching { txConn.autoCommit = true }
            runCatching { txConn.close() }
        }
    }
}
