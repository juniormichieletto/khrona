package io.khrona.store.redis

import io.khrona.core.JobBuilder
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.KhronaConfig
import io.khrona.core.Scheduler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

@Testcontainers
class RedisSchedulerMultiInstanceTest {
    private val stores = mutableListOf<RedisJobStore>()
    private val schedulers = mutableListOf<Scheduler>()
    private val scopes = mutableListOf<CoroutineScope>()

    companion object {
        @Container
        val redis = GenericContainer<Nothing>(DockerImageName.parse("redis:7-alpine")).apply {
            withCreateContainerCmdModifier { command ->
                command.withName("khrona-redis-scheduler-test")
            }
            withExposedPorts(6379)
        }
    }

    @Test
    fun `two schedulers do not duplicate interval cron or one-time executions`() = runBlocking {
        val namespace = "multi-scheduler-${UUID.randomUUID()}"
        val completed = ConcurrentHashMap.newKeySet<UUID>()
        val intervalJob = job("redis-interval-job", completed) {
            every(Duration.ofDays(365))
        }
        val cronJob = job("redis-cron-job", completed) {
            cron("0 0 1 1 *")
        }
        val oneTimeJob = job("redis-one-time-job", completed) {
            at(Instant.now().plus(Duration.ofDays(365)))
        }
        val firstStore = store(namespace)
        val secondStore = store(namespace)
        val dueExecutions = listOf(intervalJob, cronJob, oneTimeJob).map { job ->
            val executionId = UUID.randomUUID()
            JobExecution(
                id = executionId,
                jobId = job.id,
                scheduledAt = Instant.now().minusSeconds(1),
                lockKey = job.lockKey,
                payload = executionId.toString()
            )
        }
        listOf(intervalJob, cronJob, oneTimeJob).forEach { job ->
            firstStore.saveJob(job)
            secondStore.saveJob(job)
        }
        dueExecutions.forEach { firstStore.saveExecution(it) }

        val firstScheduler = scheduler(firstStore, intervalJob, cronJob, oneTimeJob)
        val secondScheduler = scheduler(secondStore, intervalJob, cronJob, oneTimeJob)
        schedulers.add(firstScheduler)
        schedulers.add(secondScheduler)

        firstScheduler.start()
        secondScheduler.start()

        awaitCompleted(completed, expected = 3)

        assertEquals(dueExecutions.map { it.id }.toSet(), completed)
    }

    @Test
    fun `two schedulers do not duplicate manual executions`() = runBlocking {
        val namespace = "manual-multi-scheduler-${UUID.randomUUID()}"
        val completed = ConcurrentHashMap.newKeySet<UUID>()
        val manualJob = job("redis-manual-job", completed) {
            at(Instant.now().plus(Duration.ofDays(365)))
        }
        val firstStore = store(namespace)
        val secondStore = store(namespace)
        val firstScheduler = scheduler(firstStore, manualJob)
        val secondScheduler = scheduler(secondStore, manualJob)
        schedulers.add(firstScheduler)
        schedulers.add(secondScheduler)

        firstScheduler.registerJob(manualJob)
        secondScheduler.registerJob(manualJob)
        firstScheduler.start()
        secondScheduler.start()

        firstScheduler.trigger(manualJob.id, payload = UUID.randomUUID().toString())
        awaitCompleted(completed, expected = 1)

        assertEquals(1, completed.size)
    }

    @AfterEach
    fun tearDown() = runBlocking {
        schedulers.forEach { it.stop() }
        scopes.forEach { it.cancel() }
        stores.forEach { it.close() }
        schedulers.clear()
        scopes.clear()
        stores.clear()
    }

    private fun store(namespace: String): RedisJobStore {
        return RedisJobStore(redisUri(), namespace = namespace).also { stores.add(it) }
    }

    private fun scheduler(store: RedisJobStore, vararg jobs: JobDefinition): Scheduler {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        scopes.add(scope)
        return Scheduler(
            KhronaConfig().apply {
                this.store = store
                pollingInterval = Duration.ofMillis(10)
                shutdownTimeout = Duration.ofSeconds(1)
                jobs.forEach { this.jobs.add(it) }
            },
            scope
        )
    }

    private fun job(id: String, completed: MutableSet<UUID>, trigger: JobBuilder.() -> Unit): JobDefinition {
        return JobBuilder(id).apply {
            trigger()
            execute {
                completed.add(UUID.fromString(it as String))
            }
        }.build()
    }

    private suspend fun awaitCompleted(completed: Set<UUID>, expected: Int) {
        withTimeout(5_000) {
            while (completed.size < expected) {
                delay(25)
            }
        }
    }

    private fun redisUri(): String = "redis://${redis.host}:${redis.getMappedPort(6379)}"
}
