package io.khrona.store.redis

import io.khrona.core.JobStore
import io.khrona.core.ExecutionStatus
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.testing.JobStoreContract
import io.lettuce.core.RedisClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.AfterEach
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Testcontainers
class RedisJobStoreTest : JobStoreContract {
    private val stores = mutableListOf<RedisJobStore>()

    companion object {
        @Container
        val redis = GenericContainer<Nothing>(DockerImageName.parse("redis:7-alpine")).apply {
            withCreateContainerCmdModifier { command ->
                command.withName("khrona-redis-test")
            }
            withExposedPorts(6379)
        }
    }

    override fun createStore(): JobStore {
        val store = RedisJobStore(redisUri(), namespace = "khrona-test-${UUID.randomUUID()}")
        stores.add(store)
        return store
    }

    @Test
    fun `isolates jobs and executions by namespace`() = runTest {
        val namespace = "namespace-isolation-${UUID.randomUUID()}"
        val firstStore = RedisJobStore(redisUri(), namespace = "$namespace-a")
        val secondStore = RedisJobStore(redisUri(), namespace = "$namespace-b")
        stores.add(firstStore)
        stores.add(secondStore)
        val firstExecution = JobExecution(jobId = "shared-job-id", scheduledAt = Instant.now().minusSeconds(1))
        val secondExecution = JobExecution(jobId = "shared-job-id", scheduledAt = Instant.now().minusSeconds(1))

        firstStore.saveExecution(firstExecution)
        secondStore.saveExecution(secondExecution)

        assertEquals(firstExecution.id, firstStore.getExecution(firstExecution.id)?.id)
        assertEquals(secondExecution.id, secondStore.getExecution(secondExecution.id)?.id)
        assertFalse(secondStore.listEligibleExecutions(Instant.now()).any { it.id == firstExecution.id })
        assertFalse(firstStore.listEligibleExecutions(Instant.now()).any { it.id == secondExecution.id })
    }

    @Test
    fun `supports explicit redis job store config`() = runTest {
        val namespace = "configured-store-${UUID.randomUUID()}"
        val store = RedisJobStore(
            RedisJobStoreConfig(
                redisUri = redisUri(),
                namespace = namespace,
                commandTimeout = Duration.ofSeconds(2),
                autoReconnect = true,
                requestQueueSize = 128,
                shutdownQuietPeriod = Duration.ZERO,
                shutdownTimeout = Duration.ofSeconds(1)
            )
        )
        stores.add(store)
        val execution = JobExecution(jobId = "configured-store-job", scheduledAt = Instant.now())

        store.saveExecution(execution)

        assertEquals(execution.id, store.getExecution(execution.id)?.id)
    }

    @Test
    fun `supports shared redis client constructor without owning client shutdown`() = runTest {
        val client = RedisClient.create(redisUri())
        val store = RedisJobStore(client, namespace = "shared-client-${UUID.randomUUID()}")
        stores.add(store)
        val execution = JobExecution(jobId = "shared-client-job", scheduledAt = Instant.now())

        store.saveExecution(execution)
        store.close()

        client.connect().use { connection ->
            assertTrue(connection.sync().ping().equals("PONG", ignoreCase = true))
        }
        client.shutdown()
        stores.remove(store)
    }

    @Test
    fun `redis oom exception exposes message and cause`() {
        val cause = RuntimeException("OOM simulated")
        val exception = KhronaRedisOomException("Redis OOM", cause)

        assertEquals("Redis OOM", exception.message)
        assertEquals(cause, exception.cause)
    }

    @Test
    fun `rejects invalid redis job store config`() {
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = "", namespace = "khrona")
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), namespace = "")
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), commandTimeout = Duration.ZERO)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), commandTimeout = Duration.ofMillis(-1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), requestQueueSize = 0)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), shutdownQuietPeriod = Duration.ofMillis(-1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), shutdownTimeout = Duration.ZERO)
        }
        assertThrows(IllegalArgumentException::class.java) {
            RedisJobStoreConfig(redisUri = redisUri(), shutdownTimeout = Duration.ofMillis(-1))
        }
    }

    @Test
    fun `claim ignores stale redis claim lock when execution state is pending`() = runTest {
        val namespace = "stale-claim-lock-${UUID.randomUUID()}"
        val store = RedisJobStore(redisUri(), namespace = namespace)
        stores.add(store)
        val execution = JobExecution(jobId = "stale-lock-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                connection.sync().set("$namespace:claim-locks:${execution.id}", "stale-worker")
            }
        }

        assertTrue(store.claimExecution(execution.id, "worker-1", Duration.ofMinutes(5)))
        assertEquals("worker-1", store.getExecution(execution.id)?.workerId)
    }

    @Test
    fun `heartbeat updates claimed index score`() = runTest {
        val namespace = "heartbeat-score-${UUID.randomUUID()}"
        val store = RedisJobStore(redisUri(), namespace = namespace)
        stores.add(store)
        val execution = JobExecution(jobId = "heartbeat-score-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        assertTrue(store.claimExecution(execution.id, "worker-1", Duration.ofMillis(100)))
        val firstScore = redisScore(namespace, "claimed", execution.id)

        assertTrue(store.heartbeat(execution.id, Duration.ofMinutes(5)))
        val secondScore = redisScore(namespace, "claimed", execution.id)

        assertTrue(secondScore > firstScore)
    }

    @Test
    fun `resetExpiredExecutions recovers running executions after heartbeat stops`() = runTest {
        val store = createStore()
        val expiredRunning = JobExecution(
            jobId = "expired-running-job",
            scheduledAt = Instant.now().minusSeconds(60),
            startedAt = Instant.now().minusSeconds(30),
            expiresAt = Instant.now().minusSeconds(1),
            status = ExecutionStatus.RUNNING,
            workerId = "lost-worker",
            lockKey = "expired-running-lock"
        )
        store.saveExecution(expiredRunning)

        val recovered = store.resetExpiredExecutions(Instant.now())

        val updated = store.getExecution(expiredRunning.id)
        assertEquals(1, recovered)
        assertEquals(ExecutionStatus.PENDING, updated?.status)
        assertEquals(null, updated?.workerId)
        assertEquals(null, updated?.startedAt)
        assertEquals(null, updated?.expiresAt)
        assertFalse(store.isLockHeld("expired-running-lock"))
        assertEquals(listOf(expiredRunning.id), store.listEligibleExecutions(Instant.now()).map { it.id })
    }

    @Test
    fun `claim and heartbeat respect configured lease durations`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "configured-lease-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        assertTrue(store.claimExecution(execution.id, "worker-1", Duration.ofMillis(150)))
        val shortLease = store.getExecution(execution.id)?.expiresAt!!
        assertTrue(shortLease <= Instant.now().plusMillis(500))

        assertTrue(store.heartbeat(execution.id, Duration.ofSeconds(2)))
        val extendedLease = store.getExecution(execution.id)?.expiresAt!!

        assertTrue(extendedLease > shortLease.plusMillis(1_000))
    }

    @Test
    fun `supersede atomically marks active executions and cleans stale lock members`() = runTest {
        val namespace = "supersede-cleanup-${UUID.randomUUID()}"
        val store = RedisJobStore(redisUri(), namespace = namespace)
        stores.add(store)
        val lockKey = "replace-lock"
        val activeExecution = JobExecution(jobId = "replace-job", scheduledAt = Instant.now(), lockKey = lockKey)
        val excludedExecution = JobExecution(jobId = "replace-job", scheduledAt = Instant.now(), lockKey = lockKey)
        val staleExecutionId = UUID.randomUUID()
        store.saveExecution(activeExecution)
        store.saveExecution(excludedExecution)
        assertTrue(store.claimExecution(activeExecution.id, "worker-1", Duration.ofMinutes(5)))
        assertTrue(store.claimExecution(excludedExecution.id, "worker-2", Duration.ofMinutes(5)))
        addStaleLockMember(namespace, lockKey, staleExecutionId)

        val superseded = store.supersedeExecutionsByLockKey(lockKey, excludeExecutionId = excludedExecution.id)

        assertEquals(listOf(activeExecution.id), superseded)
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(activeExecution.id)?.status)
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(excludedExecution.id)?.status)
        assertFalse(redisLockMembers(namespace, lockKey).contains(staleExecutionId.toString()))
    }

    @Test
    fun `claims execution once under concurrent contention`() = runTest {
        val store = createStore()
        val execution = JobExecution(jobId = "contended-job", scheduledAt = Instant.now())
        store.saveExecution(execution)

        val claims = (1..50).map { worker ->
            async(Dispatchers.Default) {
                store.claimExecution(execution.id, "worker-$worker", Duration.ofMinutes(5))
            }
        }.awaitAll()

        assertEquals(1, claims.count { it })
    }

    @Test
    fun `round trips structured payloads`() = runTest {
        val store = createStore()
        val payload = mapOf(
            "string" to "value",
            "number" to 42L,
            "boolean" to true,
            "list" to listOf("a", 1L, false),
            "nested" to mapOf("key" to null)
        )
        val execution = JobExecution(
            jobId = "payload-job",
            scheduledAt = Instant.now(),
            payload = payload
        )

        store.saveExecution(execution)

        assertEquals(payload, store.getExecution(execution.id)?.payload)
    }

    @Test
    fun `fails fast for unsupported payloads`() = runTest {
        val store = createStore()
        val execution = JobExecution(
            jobId = "bad-payload-job",
            scheduledAt = Instant.now(),
            payload = mapOf(1 to "non-string-key")
        )

        try {
            store.saveExecution(execution)
        } catch (e: IllegalArgumentException) {
            assertTrue(e.message?.contains("Unsupported map key type") == true)
            return@runTest
        }
        error("Expected unsupported payload to fail")
    }

    @Test
    fun `fails fast for unsupported numeric and object payloads`() = runTest {
        val store = createStore()

        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "nan-payload", scheduledAt = Instant.now(), payload = Double.NaN))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "object-payload", scheduledAt = Instant.now(), payload = Any()))
            }
        }
        assertThrows(IllegalArgumentException::class.java) {
            runBlocking {
                store.saveExecution(JobExecution(jobId = "null-key-payload", scheduledAt = Instant.now(), payload = mapOf(null to "value")))
            }
        }
    }

    @Test
    fun `lists jobs and returns null for missing records`() = runTest {
        val store = createStore()
        val first = job("redis-list-first")
        val second = job("redis-list-second")

        store.saveJob(first)
        store.saveJob(second)

        assertEquals(setOf(first.id, second.id), store.listJobs().map { it.id }.toSet())
        assertNull(store.getJob("missing-job"))
        assertNull(store.getExecution(UUID.randomUUID()))
    }

    @Test
    fun `status updates cover terminal and non terminal redis indexes`() = runTest {
        val store = createStore()
        val running = JobExecution(jobId = "redis-running", scheduledAt = Instant.now(), lockKey = "status-lock")
        val success = JobExecution(jobId = "redis-success", scheduledAt = Instant.now(), lockKey = "status-lock")
        store.saveExecution(running)
        store.saveExecution(success)

        store.updateExecutionStatus(running.id, ExecutionStatus.RUNNING)
        store.updateExecutionStatus(success.id, ExecutionStatus.SUCCESS, "done")

        assertEquals(ExecutionStatus.RUNNING, store.getExecution(running.id)?.status)
        assertNull(store.getExecution(running.id)?.completedAt)
        assertTrue(store.isLockHeld("status-lock"))
        assertEquals(ExecutionStatus.SUCCESS, store.getExecution(success.id)?.status)
        assertEquals("done", store.getExecution(success.id)?.error)
        assertNotNull(store.getExecution(success.id)?.completedAt)
        assertFalse(store.claimExecution(success.id, "worker", Duration.ofMinutes(1)))
        assertFalse(store.claimExecution(UUID.randomUUID(), "worker", Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(success.id, Duration.ofMinutes(1)))
        assertFalse(store.heartbeat(UUID.randomUUID(), Duration.ofMinutes(1)))

        // Missing execution status update branch
        store.updateExecutionStatus(UUID.randomUUID(), ExecutionStatus.RUNNING)

        // Heartbeat on claimed execution
        val claimedForHeartbeat = JobExecution(jobId = "redis-hb-claimed", scheduledAt = Instant.now())
        store.saveExecution(claimedForHeartbeat)
        assertTrue(store.claimExecution(claimedForHeartbeat.id, "hb-worker", Duration.ofMinutes(1)))
        assertTrue(store.heartbeat(claimedForHeartbeat.id, Duration.ofMinutes(1)))

        // Terminal statuses (MISFIRED and DEAD_LETTERED)
        val misfired = JobExecution(jobId = "redis-misfired", scheduledAt = Instant.now())
        val deadLettered = JobExecution(jobId = "redis-dead", scheduledAt = Instant.now())
        store.saveExecution(misfired)
        store.saveExecution(deadLettered)
        store.updateExecutionStatus(misfired.id, ExecutionStatus.MISFIRED)
        store.updateExecutionStatus(deadLettered.id, ExecutionStatus.DEAD_LETTERED)
        assertEquals(ExecutionStatus.MISFIRED, store.getExecution(misfired.id)?.status)
        assertEquals(ExecutionStatus.DEAD_LETTERED, store.getExecution(deadLettered.id)?.status)
    }

    @Test
    fun `eligible reset lock exclusion and supersede cover inactive redis branches`() = runTest {
        val store = createStore()
        val now = Instant.now()
        val lockKey = "redis-edge-lock"
        val expiredClaimed = JobExecution(
            jobId = "redis-expired-claimed",
            scheduledAt = now.minusSeconds(10),
            status = ExecutionStatus.CLAIMED,
            startedAt = now.minusSeconds(5),
            expiresAt = now.minusSeconds(1),
            workerId = "old-worker",
            lockKey = lockKey
        )
        val expiredRunning = JobExecution(
            jobId = "redis-expired-running",
            scheduledAt = now.minusSeconds(10),
            status = ExecutionStatus.RUNNING,
            startedAt = now.minusSeconds(5),
            expiresAt = now.minusSeconds(1),
            workerId = "old-worker-running",
            lockKey = "expired-running-lock"
        )
        val activeClaimed = JobExecution(jobId = "redis-active-claimed", scheduledAt = now, lockKey = lockKey)
        val activeRunning = JobExecution(jobId = "redis-active-running", scheduledAt = now, lockKey = lockKey)
        val excluded = JobExecution(jobId = "redis-excluded", scheduledAt = now, lockKey = lockKey)
        val pending = JobExecution(jobId = "redis-pending", scheduledAt = now, lockKey = lockKey)
        listOf(expiredClaimed, expiredRunning, activeClaimed, activeRunning, excluded, pending).forEach { store.saveExecution(it) }

        assertTrue(store.listEligibleExecutions(now, 10).map { it.id }.contains(expiredClaimed.id))
        assertTrue(store.listEligibleExecutions(now, 10).map { it.id }.contains(expiredRunning.id))
        assertEquals(2, store.resetExpiredExecutions(now))
        assertTrue(store.claimExecution(activeClaimed.id, "worker-1", Duration.ofMinutes(5)))
        assertTrue(store.claimExecution(activeRunning.id, "worker-2", Duration.ofMinutes(5)))
        store.updateExecutionStatus(activeRunning.id, ExecutionStatus.RUNNING)
        assertTrue(store.claimExecution(excluded.id, "worker-3", Duration.ofMinutes(5)))

        assertTrue(store.isLockHeld(lockKey))
        assertTrue(store.isLockHeld(lockKey, excludeExecutionId = excluded.id))
        assertFalse(store.isLockHeld("expired-running-lock"))
        assertFalse(store.isLockHeld("missing-lock"))

        val superseded = store.supersedeExecutionsByLockKey(lockKey, excludeExecutionId = excluded.id)

        assertEquals(setOf(activeClaimed.id, activeRunning.id), superseded.toSet())
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(activeClaimed.id)?.status)
        assertEquals(ExecutionStatus.SUPERSEDED, store.getExecution(activeRunning.id)?.status)
        assertNotNull(store.getExecution(activeClaimed.id)?.completedAt)
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(excluded.id)?.status)
        assertEquals(ExecutionStatus.PENDING, store.getExecution(pending.id)?.status)
        assertEquals(emptyList<UUID>(), store.supersedeExecutionsByLockKey("missing-lock"))
    }

    @Test
    fun `supports rich payloads in redis store`() = runTest {
        val store = createStore()
        val payload = mapOf(
            "str" to "hello",
            "long" to 42L,
            "double" to 3.1415,
            "boolTrue" to true,
            "boolFalse" to false,
            "nullVal" to null,
            "list" to listOf("a", 100L, 2.718, true, false, null)
        )
        val execution = JobExecution(jobId = "redis-rich-payload", scheduledAt = Instant.now(), payload = payload)
        store.saveExecution(execution)

        val retrieved = store.getExecution(execution.id)
        assertEquals(payload, retrieved?.payload)
    }

    @Test
    fun `close shuts down client when config is null`() {
        val constructor = RedisJobStore::class.java.getDeclaredConstructor(
            RedisClient::class.java,
            String::class.java,
            Boolean::class.javaPrimitiveType,
            RedisJobStoreConfig::class.java
        )
        constructor.isAccessible = true
        val client = RedisClient.create(redisUri())
        val store = constructor.newInstance(client, "reflection-ns-${UUID.randomUUID()}", true, null) as RedisJobStore
        store.close()
    }

    @Test
    fun `saveExecution covers completedAt serialization and null expiresAt for claimed and running`() = runTest {
        val store = createStore()
        val now = Instant.now()
        val completed = JobExecution(jobId = "completed-job", scheduledAt = now, completedAt = now)
        val claimedNoExp = JobExecution(jobId = "claimed-no-exp", scheduledAt = now, status = ExecutionStatus.CLAIMED, expiresAt = null)
        val runningNoExp = JobExecution(jobId = "running-no-exp", scheduledAt = now, status = ExecutionStatus.RUNNING, expiresAt = null)

        store.saveExecution(completed)
        store.saveExecution(claimedNoExp)
        store.saveExecution(runningNoExp)

        assertEquals(now, store.getExecution(completed.id)?.completedAt)
        assertEquals(ExecutionStatus.CLAIMED, store.getExecution(claimedNoExp.id)?.status)
        assertEquals(ExecutionStatus.RUNNING, store.getExecution(runningNoExp.id)?.status)
    }

    @Test
    fun `corrupted redis data structure throws RedisException caught by inRedisContext`() = runTest {
        val namespace = "wrongtype-${UUID.randomUUID()}"
        val store = RedisJobStore(redisUri(), namespace = namespace)
        stores.add(store)

        io.lettuce.core.RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                connection.sync().set("$namespace:jobs", "string-not-hash")
            }
        }

        assertThrows(io.lettuce.core.RedisException::class.java) {
            runBlocking {
                store.listJobs()
            }
        }
    }

    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
    @Test
    fun `payload decoder covers unquoted literals`() {
        val payloadClass = Class.forName("io.khrona.store.redis.RedisPayloadJson")
        val instanceField = payloadClass.getDeclaredField("INSTANCE")
        instanceField.isAccessible = true
        val instance = instanceField.get(null)
        val toAnyMethod = payloadClass.getDeclaredMethod("toAny", kotlinx.serialization.json.JsonElement::class.java)
        toAnyMethod.isAccessible = true

        val unquoted = kotlinx.serialization.json.JsonUnquotedLiteral("raw_token")
        assertEquals("raw_token", toAnyMethod.invoke(instance, unquoted))
    }

    @AfterEach
    fun tearDown() {
        stores.forEach { it.close() }
        stores.clear()
    }

    private fun redisUri(): String = "redis://${redis.host}:${redis.getMappedPort(6379)}"

    private fun job(id: String) = JobDefinition(
        id = id,
        handler = {},
        trigger = IntervalTrigger(Duration.ofMinutes(1))
    )

    private fun redisScore(namespace: String, index: String, id: UUID): Double {
        RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                return connection.sync().zscore("$namespace:$index", id.toString())
            }
        }
    }

    private fun addStaleLockMember(namespace: String, lockKey: String, id: UUID) {
        RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                connection.sync().sadd("$namespace:locks:$lockKey", id.toString())
                connection.sync().sadd("$namespace:execution-locks:$id", "$namespace:locks:$lockKey")
            }
        }
    }

    private fun redisLockMembers(namespace: String, lockKey: String): Set<String> {
        RedisClient.create(redisUri()).use { client ->
            client.connect().use { connection ->
                return connection.sync().smembers("$namespace:locks:$lockKey")
            }
        }
    }
}
