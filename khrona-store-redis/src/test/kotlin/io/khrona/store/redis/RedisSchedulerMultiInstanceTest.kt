package io.khrona.store.redis

import io.khrona.core.JobBuilder
import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.KhronaConfig
import io.khrona.core.MisfirePolicy
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
import java.util.concurrent.atomic.AtomicInteger

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

    @Test
    fun `five schedulers do not duplicate executions under redis contention`() = runBlocking {
        val namespace = "five-scheduler-contention-${UUID.randomUUID()}"
        val completed = ConcurrentHashMap.newKeySet<UUID>()
        val contentionJob = job("redis-contention-job", completed) {
            at(Instant.now().plus(Duration.ofDays(365)))
        }
        val redisStores = (1..5).map { store(namespace) }
        redisStores.forEach { it.saveJob(contentionJob) }
        val dueExecutions = (1..25).map {
            val executionId = UUID.randomUUID()
            JobExecution(
                id = executionId,
                jobId = contentionJob.id,
                scheduledAt = Instant.now().minusSeconds(1),
                payload = executionId.toString()
            )
        }
        dueExecutions.forEach { redisStores.first().saveExecution(it) }
        val redisSchedulers = redisStores.map { scheduler(it, contentionJob, pollingInterval = Duration.ofMillis(5)) }
        schedulers.addAll(redisSchedulers)

        redisSchedulers.forEach { it.start() }
        awaitCompleted(completed, expected = dueExecutions.size)

        assertEquals(dueExecutions.map { it.id }.toSet(), completed)
    }

    @Test
    fun `redis scheduler prevents FORBID overlap across scheduler instances`() = runBlocking {
        val namespace = "forbid-overlap-${UUID.randomUUID()}"
        val active = AtomicInteger(0)
        val maxActive = AtomicInteger(0)
        val completed = AtomicInteger(0)
        val lockKey = "forbid-lock"
        val forbidJob = JobBuilder("redis-forbid-job").apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            concurrencyPolicy = ConcurrencyPolicy.FORBID
            this.lockKey = lockKey
            execute {
                val current = active.incrementAndGet()
                maxActive.updateAndGet { maxOf(it, current) }
                try {
                    delay(100)
                    completed.incrementAndGet()
                } finally {
                    active.decrementAndGet()
                }
            }
        }.build()
        val firstStore = store(namespace)
        val secondStore = store(namespace)
        firstStore.saveJob(forbidJob)
        secondStore.saveJob(forbidJob)
        repeat(2) {
            firstStore.saveExecution(
                JobExecution(jobId = forbidJob.id, scheduledAt = Instant.now().minusSeconds(1), lockKey = lockKey)
            )
        }
        val firstScheduler = scheduler(firstStore, forbidJob)
        val secondScheduler = scheduler(secondStore, forbidJob)
        schedulers.add(firstScheduler)
        schedulers.add(secondScheduler)

        firstScheduler.start()
        secondScheduler.start()
        awaitCount(completed, expected = 2)

        assertEquals(1, maxActive.get())
    }

    @Test
    fun `redis scheduler REPLACE supersedes active execution without superseding replacement`() = runBlocking {
        val namespace = "replace-supersede-${UUID.randomUUID()}"
        val lockKey = "replace-lock"
        val firstStarted = AtomicInteger(0)
        val firstCancelled = AtomicInteger(0)
        val replacementCompleted = AtomicInteger(0)
        val firstExecutionId = UUID.randomUUID()
        val replacementExecutionId = UUID.randomUUID()
        val firstStore = store(namespace)
        val secondStore = store(namespace)
        val firstJob = replaceJob("redis-replace-job", lockKey) {
            firstStarted.incrementAndGet()
            try {
                delay(5_000)
            } catch (e: kotlinx.coroutines.CancellationException) {
                firstCancelled.incrementAndGet()
                throw e
            }
        }
        val secondJob = replaceJob("redis-replace-job", lockKey) {
            replacementCompleted.incrementAndGet()
        }
        firstStore.saveJob(firstJob)
        secondStore.saveJob(secondJob)
        val firstScheduler = scheduler(
            firstStore,
            firstJob,
            pollingInterval = Duration.ofMillis(500),
            executionLeaseDuration = Duration.ofMillis(200),
            heartbeatInterval = Duration.ofMillis(50)
        )
        val secondScheduler = scheduler(
            secondStore,
            secondJob,
            pollingInterval = Duration.ofMillis(10),
            executionLeaseDuration = Duration.ofMillis(200),
            heartbeatInterval = Duration.ofMillis(50)
        )
        schedulers.add(firstScheduler)
        schedulers.add(secondScheduler)

        firstScheduler.start()
        firstStore.saveExecution(
            JobExecution(id = firstExecutionId, jobId = firstJob.id, scheduledAt = Instant.now(), lockKey = lockKey)
        )
        awaitCount(firstStarted, expected = 1)

        secondScheduler.start()
        secondStore.saveExecution(
            JobExecution(id = replacementExecutionId, jobId = secondJob.id, scheduledAt = Instant.now(), lockKey = lockKey)
        )
        awaitCount(replacementCompleted, expected = 1)
        awaitCount(firstCancelled, expected = 1)

        assertEquals(ExecutionStatus.SUPERSEDED, firstStore.getExecution(firstExecutionId)?.status)
        assertEquals(ExecutionStatus.SUCCESS, firstStore.getExecution(replacementExecutionId)?.status)
    }

    @Test
    fun `redis scheduler persists retries with payload and dead letters final failure`() = runBlocking {
        val namespace = "retry-dlq-${UUID.randomUUID()}"
        val attempts = AtomicInteger(0)
        val payload = "retry-payload"
        val store = store(namespace)
        val retryJob = JobBuilder("redis-retry-job").apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            retry {
                maxAttempts = 2
                initialDelay = Duration.ofMillis(25)
                maxDelay = Duration.ofMillis(25)
                jitter = 0.0
            }
            execute {
                assertEquals(payload, it)
                attempts.incrementAndGet()
                throw IllegalStateException("retry failure")
            }
        }.build()
        val scheduler = scheduler(store, retryJob)
        schedulers.add(scheduler)
        store.saveJob(retryJob)
        store.saveExecution(
            JobExecution(jobId = retryJob.id, scheduledAt = Instant.now(), payload = payload)
        )

        scheduler.start()
        awaitCount(attempts, expected = 2)
        awaitRawExecution(namespace) { values ->
            values.any { it.contains("DEAD_LETTERED") && it.contains("retry-payload") }
        }
    }

    @Test
    fun `redis scheduler persists dead letter terminal state`() = runBlocking {
        val namespace = "dead-letter-${UUID.randomUUID()}"
        val executionId = UUID.randomUUID()
        val store = store(namespace)
        val dlqJob = JobBuilder("redis-dlq-job").apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            retry { maxAttempts = 1 }
            execute { throw IllegalStateException("permanent failure") }
        }.build()
        val scheduler = scheduler(store, dlqJob)
        schedulers.add(scheduler)
        store.saveJob(dlqJob)
        store.saveExecution(JobExecution(id = executionId, jobId = dlqJob.id, scheduledAt = Instant.now()))

        scheduler.start()
        awaitExecutionStatus(store, executionId, ExecutionStatus.DEAD_LETTERED)
    }

    @Test
    fun `redis scheduler handles misfire FIRE_NOW and IGNORE policies`() = runBlocking {
        val namespace = "misfire-${UUID.randomUUID()}"
        val fireNowId = UUID.randomUUID()
        val ignoreId = UUID.randomUUID()
        val fired = AtomicInteger(0)
        val store = store(namespace)
        val fireNowJob = JobBuilder("redis-fire-now-job").apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            misfirePolicy = MisfirePolicy.FIRE_NOW
            execute { fired.incrementAndGet() }
        }.build()
        val ignoreJob = JobBuilder("redis-ignore-job").apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            misfirePolicy = MisfirePolicy.IGNORE
            execute { error("IGNORE misfire should not execute") }
        }.build()
        val scheduler = scheduler(store, fireNowJob, ignoreJob)
        schedulers.add(scheduler)
        store.saveJob(fireNowJob)
        store.saveJob(ignoreJob)
        store.saveExecution(JobExecution(id = fireNowId, jobId = fireNowJob.id, scheduledAt = Instant.now().minusSeconds(5)))
        store.saveExecution(JobExecution(id = ignoreId, jobId = ignoreJob.id, scheduledAt = Instant.now().minusSeconds(5)))

        scheduler.config.misfireThreshold = Duration.ofMillis(1)
        scheduler.start()

        awaitCount(fired, expected = 1)
        awaitExecutionStatus(store, fireNowId, ExecutionStatus.SUCCESS)
        awaitExecutionStatus(store, ignoreId, ExecutionStatus.MISFIRED)
    }

    @Test
    fun `redis scheduler persists recurring next run after success and terminal failure`() = runBlocking {
        val namespace = "recurring-next-${UUID.randomUUID()}"
        val successExecutionId = UUID.randomUUID()
        val failureExecutionId = UUID.randomUUID()
        val store = store(namespace)
        val successJob = JobBuilder("redis-recurring-success-job").apply {
            every(Duration.ofMillis(250))
            execute {}
        }.build()
        val failureJob = JobBuilder("redis-recurring-failure-job").apply {
            every(Duration.ofMillis(250))
            retry { maxAttempts = 1 }
            execute { throw IllegalStateException("terminal") }
        }.build()
        val scheduler = scheduler(store, successJob, failureJob)
        schedulers.add(scheduler)
        val successScheduledAt = Instant.now().minusMillis(500)
        val failureScheduledAt = Instant.now().minusMillis(500)
        val successNext = successJob.trigger.nextExecutionTime(successScheduledAt.plusMillis(1))!!
            .truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        val failureNext = failureJob.trigger.nextExecutionTime(failureScheduledAt.plusMillis(1))!!
            .truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
        store.saveJob(successJob)
        store.saveJob(failureJob)
        store.saveExecution(JobExecution(id = successExecutionId, jobId = successJob.id, scheduledAt = successScheduledAt))
        store.saveExecution(JobExecution(id = failureExecutionId, jobId = failureJob.id, scheduledAt = failureScheduledAt))

        scheduler.start()

        awaitExecutionStatus(store, successExecutionId, ExecutionStatus.SUCCESS)
        awaitExecutionStatus(store, failureExecutionId, ExecutionStatus.DEAD_LETTERED)
        awaitExecutionExists(store, UUID.nameUUIDFromBytes("${successJob.id}:$successNext".toByteArray()))
        awaitExecutionExists(store, UUID.nameUUIDFromBytes("${failureJob.id}:$failureNext".toByteArray()))
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

    private fun scheduler(
        store: RedisJobStore,
        vararg jobs: JobDefinition,
        pollingInterval: Duration = Duration.ofMillis(10),
        executionLeaseDuration: Duration = Duration.ofMinutes(5),
        heartbeatInterval: Duration? = null
    ): Scheduler {
        val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        scopes.add(scope)
        return Scheduler(
            KhronaConfig().apply {
                this.store = store
                this.pollingInterval = pollingInterval
                this.executionLeaseDuration = executionLeaseDuration
                this.heartbeatInterval = heartbeatInterval
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

    private fun replaceJob(id: String, lockKey: String, handler: suspend (Any?) -> Unit): JobDefinition {
        return JobBuilder(id).apply {
            at(Instant.now().plus(Duration.ofDays(365)))
            concurrencyPolicy = ConcurrencyPolicy.REPLACE
            this.lockKey = lockKey
            execute(handler)
        }.build()
    }

    private suspend fun awaitCompleted(completed: Set<UUID>, expected: Int) {
        withTimeout(5_000) {
            while (completed.size < expected) {
                delay(25)
            }
        }
    }

    private suspend fun awaitCount(counter: AtomicInteger, expected: Int) {
        withTimeout(5_000) {
            while (counter.get() < expected) {
                delay(25)
            }
        }
    }

    private suspend fun awaitExecutionStatus(store: RedisJobStore, id: UUID, status: ExecutionStatus) {
        withTimeout(5_000) {
            while (store.getExecution(id)?.status != status) {
                delay(25)
            }
        }
    }

    private suspend fun awaitExecutionExists(store: RedisJobStore, id: UUID) {
        withTimeout(5_000) {
            while (store.getExecution(id) == null) {
                delay(25)
            }
        }
    }

    private suspend fun awaitRawExecution(namespace: String, predicate: (List<String>) -> Boolean) {
        withTimeout(5_000) {
            while (!predicate(rawExecutions(namespace))) {
                delay(25)
            }
        }
    }

    private fun rawExecutions(namespace: String): List<String> {
        io.lettuce.core.RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                return connection.sync().hvals("$namespace:executions")
            }
        }
    }

    private fun redisUri(): String = "redis://${redis.host}:${redis.getMappedPort(6379)}"
}
