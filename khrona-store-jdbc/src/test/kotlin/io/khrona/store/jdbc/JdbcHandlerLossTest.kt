package io.khrona.store.jdbc

import io.khrona.core.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

class JdbcHandlerLossTest {
    private lateinit var dataSource: JdbcDataSource
    private lateinit var store: JdbcJobStore

    @BeforeEach
    fun setup() {
        dataSource = JdbcDataSource().apply {
            setURL("jdbc:h2:mem:khrona_test;DB_CLOSE_DELAY=-1")
            user = "sa"
            password = ""
        }
        store = JdbcJobStore(dataSource)
        store.migrate()
    }

    @Test
    fun `should correctly execute handler after reloading job from JDBC when using Scheduler`() = kotlinx.coroutines.test.runTest {
        val executed = AtomicBoolean(false)
        val config = KhronaConfig().apply {
            this.store = this@JdbcHandlerLossTest.store
            // Use a short polling interval for fast tests
            this.pollingInterval = Duration.ofMillis(100)
            job("test-scheduler-fix") {
                every(Duration.ofMinutes(1))
                execute {
                    executed.set(true)
                }
            }
        }
        
        // Use a real scheduler but with the test's scope and system clock
        val scheduler = Scheduler(config, backgroundScope, Clock.systemUTC())
        
        // 1. Manually save the job definition to ensure referential integrity for the execution
        store.saveJob(config.jobs.first())
        
        // 2. Start scheduler (this registers the handler in HandlerRegistry)
        scheduler.start()
        
        // 3. Prepare an execution in the DB that is already eligible
        val jobDef = config.jobs.first()
        val execution = JobExecution(
            jobId = jobDef.id, 
            scheduledAt = Instant.now().minusSeconds(10)
        )
        store.saveExecution(execution)
        
        // 3. Wait for the polling loop to pick it up.
        // Since we are using backgroundScope and the real Scheduler uses delay(),
        // we just need to wait long enough.
        var attempts = 0
        while (!executed.get() && attempts < 50) {
            kotlinx.coroutines.delay(100)
            attempts++
        }
        
        // 4. Verification
        assertTrue(executed.get(), "Handler should have been executed via HandlerRegistry lookup after $attempts attempts")
        
        scheduler.stop()
    }
}
