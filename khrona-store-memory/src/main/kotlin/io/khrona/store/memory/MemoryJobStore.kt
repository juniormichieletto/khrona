package io.khrona.store.memory

import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobStore
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class MemoryJobStore : JobStore {
    private val jobs = ConcurrentHashMap<String, JobDefinition>()
    private val executions = ConcurrentHashMap<UUID, JobExecution>()

    override suspend fun saveJob(job: JobDefinition) {
        jobs[job.id] = job
    }

    override suspend fun getJob(jobId: String): JobDefinition? = jobs[jobId]

    override suspend fun listJobs(): List<JobDefinition> = jobs.values.toList()

    override suspend fun saveExecution(execution: JobExecution) {
        executions[execution.id] = execution
    }

    override suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String?) {
        executions.computeIfPresent(id) { _, exec ->
            exec.copy(
                status = status,
                error = error,
                completedAt = if (status == ExecutionStatus.SUCCESS || status == ExecutionStatus.FAILED) Instant.now() else exec.completedAt
            )
        }
    }

    override suspend fun getExecution(id: UUID): JobExecution? = executions[id]

    override suspend fun listEligibleExecutions(now: Instant): List<JobExecution> {
        return executions.values.filter { 
            it.status == ExecutionStatus.PENDING && it.scheduledAt <= now 
        }
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
        var claimed = false
        executions.computeIfPresent(id) { _, exec ->
            if (exec.status == ExecutionStatus.PENDING) {
                claimed = true
                exec.copy(
                    status = ExecutionStatus.CLAIMED,
                    workerId = workerId,
                    startedAt = Instant.now()
                )
            } else {
                exec
            }
        }
        return claimed
    }
}
