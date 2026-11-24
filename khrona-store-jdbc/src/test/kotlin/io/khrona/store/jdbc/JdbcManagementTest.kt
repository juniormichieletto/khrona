package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.ExecutionStatus
import io.khrona.core.KhronaConfig
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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
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
}
