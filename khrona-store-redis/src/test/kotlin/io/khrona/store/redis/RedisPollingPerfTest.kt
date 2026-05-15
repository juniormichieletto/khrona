package io.khrona.store.redis

import io.khrona.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import java.time.Duration
import java.util.*

/**
 * Performance test for Redis-backed Khrona schedulers.
 * Measures the impact of Redis polling on CPU and Memory.
 */
@Disabled("Manual performance test - requires a running Redis instance.")
class RedisPollingPerfTest {

    private val runtime = Runtime.getRuntime()
    private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

    @Test
    fun runRedisPerf() = runBlocking {
        // Assumes Redis is running (e.g., via ./docker-start.sh)
        val config = RedisJobStoreConfig(
            redisUri = "redis://:khrona_dev_password@localhost:6379/0",
            namespace = "khrona-perf"
        )
        val store = try {
            RedisJobStore(config)
        } catch (e: Exception) {
            println("Skipping Redis test: Could not connect to Redis at ${config.redisUri}. Ensure ./docker-start.sh is running.")
            return@runBlocking
        }

        val intervals = listOf(
            Duration.ofMillis(100),
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
            Duration.ofMinutes(1)
        )

        println("\n--- Redis Performance Test ---")
        println("Settings: 10 jobs, 10s measurement window per interval.")
        println("----------------------------------------------------------------------")
        println("%-15s | %-12s | %-12s".format("Interval", "Avg CPU (%)", "Max Memory (MB)"))
        println("----------------------------------------------------------------------")

        intervals.forEach { interval ->
            val khronaConfig = KhronaConfig().apply {
                this.store = store
                this.pollingInterval = interval
                
                (1..10).forEach { i ->
                    job("perf-redis-$i") {
                        every(Duration.ofMinutes(5))
                        execute { /* No-op */ }
                    }
                }
            }

            val scheduler = Scheduler(khronaConfig)
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
        store.close()
    }
}
