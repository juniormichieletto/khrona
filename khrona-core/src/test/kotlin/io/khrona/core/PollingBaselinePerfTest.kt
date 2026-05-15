package io.khrona.core

import io.khrona.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

/**
 * Baseline performance test for the Khrona scheduler loop.
 * Measures CPU and Memory overhead of polling without a real database.
 */
@Disabled("Manual performance test - run locally to measure scheduler overhead.")
class PollingBaselinePerfTest {

    private val runtime = Runtime.getRuntime()
    private val osBean = ManagementFactory.getOperatingSystemMXBean() as com.sun.management.OperatingSystemMXBean

    @Test
    fun runPerformanceTest() = runBlocking {
        val intervals = listOf(
            Duration.ofMillis(100),
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
            Duration.ofMinutes(1)
        )

        println("\n--- Polling Baseline Performance Test ---")
        println("Settings: 10 jobs, 10s measurement window per interval.")
        println("----------------------------------------------------------------------")
        println("%-15s | %-12s | %-12s".format("Interval", "Avg CPU (%)", "Max Memory (MB)"))
        println("----------------------------------------------------------------------")

        intervals.forEach { interval ->
            val store = MinimalJobStore()
            val config = KhronaConfig().apply {
                this.store = store
                this.pollingInterval = interval
                
                (1..10).forEach { i ->
                    job("perf-job-$i") {
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
    }

    private class MinimalJobStore : JobStore {
        private val jobs = ConcurrentHashMap<String, JobDefinition>()
        override suspend fun saveJob(job: JobDefinition) { jobs[job.id] = job }
        override suspend fun getJob(jobId: String): JobDefinition? = jobs[jobId]
        override suspend fun listJobs(): List<JobDefinition> = jobs.values.toList()
        override suspend fun saveExecution(execution: JobExecution) {}
        override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?) {}
        override suspend fun getExecution(id: UUID): JobExecution? = null
        override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> = emptyList()
        override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: java.time.Duration): Boolean = false
        override suspend fun heartbeat(id: UUID, leaseDuration: java.time.Duration): Boolean = false
        override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean = false
        override suspend fun resetExpiredExecutions(now: Instant): Int = 0
        override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> = emptyList()
    }
}
