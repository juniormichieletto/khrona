package io.khrona.store.redis

import io.khrona.core.JobStore
import io.khrona.core.ExecutionStatus
import io.khrona.core.JobExecution
import io.khrona.core.testing.JobStoreContract
import io.lettuce.core.RedisClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertEquals
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

    @AfterEach
    fun tearDown() {
        stores.forEach { it.close() }
        stores.clear()
    }

    private fun redisUri(): String = "redis://${redis.host}:${redis.getMappedPort(6379)}"

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
