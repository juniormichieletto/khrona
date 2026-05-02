package io.khrona.store.jdbc

import io.khrona.core.*
import kotlinx.coroutines.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class JdbcEndToEndTest {
    private lateinit var dataSource: JdbcDataSource
    private lateinit var store: JdbcJobStore

    @BeforeEach
    fun setup() {
        dataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:khrona_e2e_test;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }
        store = JdbcJobStore(dataSource)
        store.migrate()
    }

    @Test
    fun `should handle full job lifecycle across scheduler restart with JDBC`() = runBlocking {
        val capturedPayload = AtomicReference<Any?>(null)
        val jobId = "e2e-report-job"
        val testPayload = mapOf("reportId" to 42L, "type" to "FINANCIAL")
        
        val testScope = CoroutineScope(Dispatchers.Default + Job())

        // PHASE 1: Initial Registration & Execution
        run {
            val config = KhronaConfig().apply {
                this.store = this@JdbcEndToEndTest.store
                this.pollingInterval = Duration.ofMillis(100)
                job(jobId) {
                    once()
                    execute { payload ->
                        capturedPayload.set(payload)
                    }
                }
            }
            val scheduler = Scheduler(config, testScope)
            
            // Manually register to ensure it's in the store (suspendable)
            config.jobs.forEach { scheduler.registerJob(it) }
            
            scheduler.start()

            // Manually trigger with payload
            scheduler.trigger(jobId, payload = testPayload)

            // Wait for execution
            var attempts = 0
            while (capturedPayload.get() == null && attempts < 50) {
                delay(100)
                attempts++
            }

            assertEquals(testPayload, capturedPayload.get(), "Payload should match exactly after first run")
            scheduler.stop()
        }

        // PHASE 2: Restart Simulation
        capturedPayload.set(null)
        
        run {
            val config = KhronaConfig().apply {
                this.store = this@JdbcEndToEndTest.store
                this.pollingInterval = Duration.ofMillis(100)
                job(jobId) {
                    once()
                    execute { payload ->
                        capturedPayload.set(payload)
                    }
                }
            }
            
            val scheduler = Scheduler(config, testScope)
            config.jobs.forEach { scheduler.registerJob(it) }
            scheduler.start()
            
            scheduler.trigger(jobId, payload = testPayload)
            
            var attempts = 0
            while (capturedPayload.get() == null && attempts < 50) {
                delay(100)
                attempts++
            }

            assertEquals(testPayload, capturedPayload.get(), "Payload should match exactly after restart")
            scheduler.stop()
        }
        
        testScope.cancel()
    }
}
