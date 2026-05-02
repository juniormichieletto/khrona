package io.khrona.store.jdbc

import io.khrona.core.*
import kotlinx.serialization.Serializable
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*

class JdbcPayloadSerializationTest {
    private lateinit var dataSource: JdbcDataSource
    private lateinit var store: JdbcJobStore

    @Serializable
    data class ComplexPayload(val id: Int, val name: String, val tags: List<String>)

    @BeforeEach
    fun setup() {
        dataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:khrona_payload_test;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }
        store = JdbcJobStore(dataSource)
        store.migrate()
    }

    @Test
    fun `should preserve complex payload structure in JDBC`() = kotlinx.coroutines.test.runTest {
        val jobDef = JobDefinition(
            id = "test-job",
            trigger = IntervalTrigger(Duration.ofMinutes(1)),
            handler = {}
        )
        store.saveJob(jobDef)

        val payload = mapOf("key" to "value", "nested" to mapOf("id" to 123))
        val execution = JobExecution(
            id = UUID.randomUUID(),
            jobId = "test-job",
            scheduledAt = Instant.now(),
            payload = payload
        )

        // 1. Save execution
        store.saveExecution(execution)

        // 2. Reload from DB
        val reloaded = store.getExecution(execution.id)
        assertNotNull(reloaded)
        
        // 3. Verification
        val reloadedPayload = reloaded?.payload
        println("Reloaded payload: $reloadedPayload (class: ${reloadedPayload?.javaClass})")
        
        // This is expected to fail or show the lossy nature in the print
        assertTrue(reloadedPayload is Map<*, *>, "Payload should be a Map")
        assertEquals("value", (reloadedPayload as Map<*, *>)["key"])
    }
}
