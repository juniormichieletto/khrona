package io.khrona.store.redis

import io.khrona.core.JobStore
import io.khrona.core.JobExecution
import io.khrona.core.testing.JobStoreContract
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
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
    private lateinit var store: RedisJobStore

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
        val redisUri = "redis://${redis.host}:${redis.getMappedPort(6379)}"
        store = RedisJobStore(redisUri, namespace = "khrona-test-${UUID.randomUUID()}")
        return store
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
        if (::store.isInitialized) {
            store.close()
        }
    }
}
