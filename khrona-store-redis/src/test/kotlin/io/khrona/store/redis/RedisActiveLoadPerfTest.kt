package io.khrona.store.redis

import io.khrona.core.ConcurrencyPolicy
import io.khrona.core.JobExecution
import io.khrona.core.KhronaConfig
import io.khrona.core.Scheduler
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import java.lang.management.ManagementFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Active-load and multi-node backlog-drain benchmarks for `RedisJobStore`.
 *
 * Excluded from default `./gradlew test` runs via `@Tag("perf")`.
 * Run explicitly with:
 * ```bash
 * ./gradlew :khrona-store-redis:perfTest
 * ```
 */
@Tag("perf")
class RedisActiveLoadPerfTest {

    private val runtime = Runtime.getRuntime()
    private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

    @Test
    fun `redis active recurring load measures throughput cpu and memory`() = runBlocking {
        GenericContainer("redis:7-alpine").withExposedPorts(6379).use { redis ->
            redis.start()
            val store = RedisJobStore(
                RedisJobStoreConfig(
                    redisUri = "redis://${redis.host}:${redis.getMappedPort(6379)}/0",
                    namespace = "khrona-perf-active-${UUID.randomUUID()}"
                )
            )
            try {
                val measuring = AtomicBoolean(false)
                val completed = AtomicInteger(0)

                val config = KhronaConfig().apply {
                    this.store = store
                    this.pollingInterval = Duration.ofMillis(100)
                    this.pollBatchSize = 100
                    (1..100).forEach { i ->
                        job("active-redis-$i") {
                            every(Duration.ofSeconds(1))
                            execute {
                                if (measuring.get()) completed.incrementAndGet()
                            }
                        }
                    }
                }

                val scheduler = Scheduler(config)
                scheduler.start()
                delay(3000)

                val cpuSamples = mutableListOf<Double>()
                var maxMemoryMb = 0L
                measuring.set(true)
                val startMs = System.currentTimeMillis()
                while (System.currentTimeMillis() - startMs < 10_000L) {
                    val cpu = osBean.processCpuLoad * 100.0
                    if (cpu >= 0) cpuSamples.add(cpu)
                    val memMb = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
                    if (memMb > maxMemoryMb) maxMemoryMb = memMb
                    delay(250)
                }
                measuring.set(false)
                val elapsedSec = (System.currentTimeMillis() - startMs) / 1000.0
                scheduler.stop()

                val sortedCpu = cpuSamples.sorted()
                val avgCpu = if (sortedCpu.isNotEmpty()) sortedCpu.average() else 0.0
                val p95Cpu = if (sortedCpu.isNotEmpty()) sortedCpu[((sortedCpu.size - 1) * 0.95).toInt()] else 0.0
                val throughput = completed.get() / elapsedSec

                println("\n--- Redis Active Load Test (100 jobs @ 1s, poll=100ms, 10s window after 3s warmup) ---")
                println("Completed: ${completed.get()} | Throughput: %.1f exec/s | Avg CPU: %.2f%% | p95 CPU: %.2f%% | Max Mem: %d MB".format(throughput, avgCpu, p95Cpu, maxMemoryMb))

                assertTrue(completed.get() > 0, "Expected executions to complete during measurement window")
            } finally {
                store.close()
            }
        }
    }

    @Test
    fun `redis multi-node backlog drain drains 1000 due executions`() = runBlocking {
        GenericContainer("redis:7-alpine").withExposedPorts(6379).use { redis ->
            redis.start()
            val uri = "redis://${redis.host}:${redis.getMappedPort(6379)}/0"
            val namespace = "khrona-perf-drain-${UUID.randomUUID()}"
            val store1 = RedisJobStore(RedisJobStoreConfig(redisUri = uri, namespace = namespace))
            val store2 = RedisJobStore(RedisJobStoreConfig(redisUri = uri, namespace = namespace))
            try {
                val totalJobs = 1000
                val completed = AtomicInteger(0)

                fun buildConfig(store: RedisJobStore) = KhronaConfig().apply {
                    this.store = store
                    this.pollingInterval = Duration.ofMillis(50)
                    this.pollBatchSize = 100
                    job("backlog-redis") {
                        concurrencyPolicy = ConcurrencyPolicy.ALLOW
                        once()
                        execute { completed.incrementAndGet() }
                    }
                }

                val config1 = buildConfig(store1)
                val config2 = buildConfig(store2)
                store1.saveJob(config1.jobs.first())

                val dueAt = Instant.now().minusSeconds(5)
                repeat(totalJobs) {
                    store1.saveExecution(JobExecution(jobId = "backlog-redis", scheduledAt = dueAt))
                }

                val scheduler1 = Scheduler(config1)
                val scheduler2 = Scheduler(config2)
                val startMs = System.currentTimeMillis()
                var maxMemoryMb = 0L
                scheduler1.start()
                scheduler2.start()

                while (completed.get() < totalJobs && System.currentTimeMillis() - startMs < 60_000L) {
                    val memMb = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
                    if (memMb > maxMemoryMb) maxMemoryMb = memMb
                    delay(50)
                }
                val drainMs = System.currentTimeMillis() - startMs
                scheduler1.stop()
                scheduler2.stop()

                val throughput = completed.get() / (drainMs / 1000.0).coerceAtLeast(0.001)
                println("\n--- Redis 2-Node Backlog Drain ($totalJobs due executions, batch=100, poll=50ms) ---")
                println("Drained: ${completed.get()}/$totalJobs in ${drainMs}ms (%.1f exec/s) | Max Mem: %d MB".format(throughput, maxMemoryMb))

                assertEquals(totalJobs, completed.get(), "All seeded backlog executions should drain without duplication")
            } finally {
                store1.close()
                store2.close()
            }
        }
    }
}
