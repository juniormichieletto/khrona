package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class SchedulerTest {

    class MockJobStore : JobStore {
        val jobs = ConcurrentHashMap<String, JobDefinition>()
        val executions = ConcurrentHashMap<UUID, JobExecution>()
        val updatedStatuses = mutableListOf<Pair<UUID, ExecutionStatus>>()

        override suspend fun saveJob(job: JobDefinition) {
            jobs[job.id] = job
        }

        override suspend fun getJob(jobId: String): JobDefinition? = jobs[jobId]

        override suspend fun listJobs(): List<JobDefinition> = jobs.values.toList()

        override suspend fun saveExecution(execution: JobExecution) {
            executions[execution.id] = execution
        }

        override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?) {
            executions.computeIfPresent(id) { _, exec -> exec.copy(status = status) }
            updatedStatuses.add(id to status)
        }

        override suspend fun getExecution(id: UUID): JobExecution? = executions[id]

        override suspend fun listEligibleExecutions(now: Instant): List<JobExecution> {
            return executions.values.filter { it.status == ExecutionStatus.PENDING && it.scheduledAt <= now }
        }

        override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
            var claimed = false
            executions.computeIfPresent(id) { _, exec ->
                if (exec.status == ExecutionStatus.PENDING) {
                    claimed = true
                    exec.copy(status = ExecutionStatus.CLAIMED)
                } else {
                    exec
                }
            }
            return claimed
        }
    }

    @Test
    fun `scheduler should execute eligible jobs`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            job("test-job") {
                every(Duration.ofMinutes(1))
                execute {
                    // Job logic
                }
            }
        }
        
        val scheduler = Scheduler(config, this)
        
        // Setup an eligible execution
        val execution = JobExecution(jobId = "test-job", scheduledAt = Instant.now().minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        
        // Wait for polling and execution
        advanceTimeBy(2000)
        
        // Check if status reached SUCCESS
        // We might need a bit of real delay or more advanceTime if things are launched in other scopes
        // But since we pass 'this' as scope, it should be controlled by runTest
        
        yield() // Allow other coroutines to run
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.SUCCESS, updated?.status)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should handle job failure`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            job("fail-job") {
                every(Duration.ofMinutes(1))
                execute {
                    throw RuntimeException("Boom")
                }
            }
        }
        
        val scheduler = Scheduler(config, this)
        
        val execution = JobExecution(jobId = "fail-job", scheduledAt = Instant.now().minusSeconds(10))
        store.saveExecution(execution)
        store.saveJob(config.jobs.first())

        scheduler.start()
        advanceTimeBy(2000)
        yield()
        
        val updated = store.getExecution(execution.id)
        assertEquals(ExecutionStatus.FAILED, updated?.status)
        
        scheduler.stop()
    }
}
