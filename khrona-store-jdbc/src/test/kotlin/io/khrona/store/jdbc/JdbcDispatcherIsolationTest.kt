package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.IntervalTrigger
import io.khrona.core.JobDefinition
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.sql.Connection
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

class JdbcDispatcherIsolationTest {

    private lateinit var dataSource: HikariDataSource
    private val testDispatcher = Executors.newSingleThreadExecutor { r ->
        Thread(r, "jdbc-test-pool")
    }.asCoroutineDispatcher()

    @BeforeEach
    fun setup() {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:isolation_test;DB_CLOSE_DELAY=-1"
            driverClassName = "org.h2.Driver"
        }
        dataSource = HikariDataSource(config)
    }

    @AfterEach
    fun tearDown() {
        dataSource.close()
    }

    @Test
    fun `should run JDBC operations on the configured dispatcher`() = runBlocking {
        val store = JdbcJobStore(dataSource, dispatcher = testDispatcher)
        store.migrate()

        val job = JobDefinition(
            id = "test-job",
            handler = { },
            trigger = IntervalTrigger(Duration.ofMinutes(1))
        )

        // Use a different dispatcher for the caller
        val callerDispatcher = Executors.newSingleThreadExecutor { r ->
            Thread(r, "caller-thread")
        }.asCoroutineDispatcher()

        withContext(callerDispatcher) {
            assertTrue(Thread.currentThread().name.contains("caller-thread"))
            
            // We need a way to verify the thread INSIDE the store.
            // Let's use a custom DataSource that captures the thread name.
            val capturingDataSource = object : DataSource by dataSource {
                override fun getConnection(): java.sql.Connection {
                    capturedThreadName = Thread.currentThread().name
                    return dataSource.connection
                }
            }
            
            val storeWithCapturingDS = JdbcJobStore(capturingDataSource, dispatcher = testDispatcher)
            storeWithCapturingDS.saveJob(job)
            
            assertTrue(capturedThreadName?.contains("jdbc-test-pool") == true)
            assertTrue(Thread.currentThread().name.contains("caller-thread"))
        }
    }

    @Test
    fun `first operation should resolve dialect before opening operation connection`() = runBlocking {
        dataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute("CREATE TABLE khrona_jobs (id VARCHAR(255) PRIMARY KEY, definition_json TEXT NOT NULL)")
            }
        }

        val activeConnections = AtomicInteger(0)
        val singleConnectionDataSource = object : DataSource by dataSource {
            override fun getConnection(): Connection {
                if (activeConnections.incrementAndGet() > 1) {
                    activeConnections.decrementAndGet()
                    throw IllegalStateException("Nested connection acquisition")
                }

                val connection = dataSource.connection
                return Proxy.newProxyInstance(
                    Connection::class.java.classLoader,
                    arrayOf(Connection::class.java)
                ) { _, method, args ->
                    if (method.name == "close") {
                        try {
                            method.invoke(connection, *(args ?: emptyArray()))
                        } finally {
                            activeConnections.decrementAndGet()
                        }
                    } else {
                        method.invoke(connection, *(args ?: emptyArray()))
                    }
                } as Connection
            }
        }

        val store = JdbcJobStore(singleConnectionDataSource, dispatcher = testDispatcher)
        store.saveJob(
            JobDefinition(
                id = "test-job",
                handler = { },
                trigger = IntervalTrigger(Duration.ofMinutes(1))
            )
        )
    }

    private var capturedThreadName: String? = null
}
