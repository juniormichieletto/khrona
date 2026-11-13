package io.khrona.core

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Active-load and backlog-drain performance benchmarks for the core scheduler.
 *
 * Excluded from default `./gradlew test` runs via `@Tag("perf")`.
 * Run explicitly with:
 * ```bash
 * ./gradlew :khrona-core:perfTest
 * ```
 */
@Tag("perf")
class ActiveLoadPerfTest {

    private val runtime = Runtime.getRuntime()
    private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

    @Test
    fun `active recurring load measures throughput cpu and memory`() = runBlocking {
        val store = MockJobStore()
        val measuring = AtomicBoolean(false)
        val completed = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(100)
            this.pollBatchSize = 100
            (1..100).forEach { i ->
                job("active-core-$i") {
                    every(Duration.ofSeconds(1))
                    execute {
                        if (measuring.get()) {
                            completed.incrementAndGet()
                        }
                    }
                }
            }
        }

        val scheduler = Scheduler(config)
        scheduler.start()

        // Warmup window to allow JIT and initial scheduling to settle
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

        println("\n--- Core Active Load Test (100 jobs @ 1s, poll=100ms, 10s window after 3s warmup) ---")
        println("Completed: ${completed.get()} | Throughput: %.1f exec/s | Avg CPU: %.2f%% | p95 CPU: %.2f%% | Max Mem: %d MB".format(throughput, avgCpu, p95Cpu, maxMemoryMb))

        assertTrue(completed.get() > 0, "Expected executions to complete during measurement window")
    }

    @Test
    fun `backlog drain measures time to drain 1000 due executions`() = runBlocking {
        val store = MockJobStore()
        val totalJobs = 1000
        val completed = AtomicInteger(0)

        val config = KhronaConfig().apply {
            this.store = store
            this.pollingInterval = Duration.ofMillis(50)
            this.pollBatchSize = 100
            job("backlog-core") {
                concurrencyPolicy = ConcurrencyPolicy.ALLOW
                once()
                execute {
                    completed.incrementAndGet()
                }
            }
        }

        val scheduler = Scheduler(config)
        // Register job definition before seeding backlog
        store.saveJob(config.jobs.first())
        val dueAt = Instant.now().minusSeconds(5)
        repeat(totalJobs) {
            store.saveExecution(JobExecution(jobId = "backlog-core", scheduledAt = dueAt))
        }

        val startMs = System.currentTimeMillis()
        var maxMemoryMb = 0L
        scheduler.start()
        while (completed.get() < totalJobs && System.currentTimeMillis() - startMs < 30_000L) {
            val memMb = (runtime.totalMemory() - runtime.freeMemory()) / 1024 / 1024
            if (memMb > maxMemoryMb) maxMemoryMb = memMb
            delay(50)
        }
        val drainMs = System.currentTimeMillis() - startMs
        scheduler.stop()

        val throughput = completed.get() / (drainMs / 1000.0).coerceAtLeast(0.001)
        println("\n--- Core Backlog Drain Test ($totalJobs due executions, batch=100, poll=50ms) ---")
        println("Drained: ${completed.get()}/$totalJobs in ${drainMs}ms (%.1f exec/s) | Max Mem: %d MB".format(throughput, maxMemoryMb))

        assertEquals(totalJobs, completed.get(), "All seeded backlog executions should drain")
    }
}
