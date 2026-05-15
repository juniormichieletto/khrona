package io.khrona.store.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.khrona.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.containers.PostgreSQLContainer
import java.lang.management.ManagementFactory
import java.time.Duration
import java.time.Instant
import java.util.*

/**
 * Performance test for JDBC-backed Khrona schedulers.
 * Measures the impact of database polling on CPU and Memory.
 */
@Disabled("Manual performance test - requires Docker and takes several minutes.")
class JdbcPollingPerfTest {

    private val runtime = Runtime.getRuntime()
    private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

    @Test
    fun runH2Perf() = runBenchmark("H2 (In-Memory)") {
        val config = HikariConfig().apply {
            jdbcUrl = "jdbc:h2:mem:perf;DB_CLOSE_DELAY=-1"
            driverClassName = "org.h2.Driver"
        }
        HikariDataSource(config)
    }

    @Test
    fun runPostgresPerf() = runBenchmark("PostgreSQL (Docker)") {
        val container = PostgreSQLContainer("postgres:16-alpine").apply { start() }
        val config = HikariConfig().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
        }
        HikariDataSource(config)
    }

    @Test
    fun runMySqlPerf() = runBenchmark("MySQL (Docker)") {
        val container = MySQLContainer("mysql:8.0").apply { start() }
        val config = HikariConfig().apply {
            jdbcUrl = container.jdbcUrl
            username = container.username
            password = container.password
        }
        HikariDataSource(config)
    }

    private fun runBenchmark(name: String, dataSourceProvider: () -> HikariDataSource) = runBlocking {
        val dataSource = dataSourceProvider()
        val store = JdbcJobStore(dataSource)
        store.migrate()

        val intervals = listOf(
            Duration.ofMillis(100),
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
            Duration.ofMinutes(1)
        )

        println("\n--- JDBC Performance Test: $name ---")
        println("Settings: 10 jobs, 10s measurement window per interval.")
        println("----------------------------------------------------------------------")
        println("%-15s | %-12s | %-12s".format("Interval", "Avg CPU (%)", "Max Memory (MB)"))
        println("----------------------------------------------------------------------")

        intervals.forEach { interval ->
            val config = KhronaConfig().apply {
                this.store = store
                this.pollingInterval = interval
                
                (1..10).forEach { i ->
                    job("perf-$name-$i") {
                        every(Duration.ofMinutes(5))
                        execute { /* No-op */ }
                    }
                }
            }

            val scheduler = Scheduler(config)
            scheduler.start()

            val cpuSamples = mutableListOf<Double>()
            var maxMemory = 0L
            val start = System.currentTimeMillis()
            
            while (System.currentTimeMillis() - start < 10000L) {
                val cpuLoad = osBean.processCpuLoad * 100.0
                if (cpuLoad >= 0) cpuSamples.add(cpuLoad)
                val currentMem = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
                if (currentMem > maxMemory) maxMemory = currentMem
                delay(500)
            }

            scheduler.stop()
            val avgCpu = if (cpuSamples.isNotEmpty()) cpuSamples.average() else 0.0
            println("%-15s | %-12.2f | %-12d".format(interval.toString(), avgCpu, maxMemory))
            
            System.gc()
            delay(1000)
        }
        println("----------------------------------------------------------------------\n")
        dataSource.close()
    }
}
