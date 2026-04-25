package io.khrona.core

import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap

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
        executions.computeIfPresent(id) { _, exec -> exec.copy(status = status, error = error) }
        updatedStatuses.add(id to status)
    }

    override suspend fun getExecution(id: UUID): JobExecution? = executions[id]

    override suspend fun listEligibleExecutions(now: Instant): List<JobExecution> {
        return executions.values.filter { it ->
            val expiresAt = it.expiresAt
            (it.status == ExecutionStatus.PENDING || ((it.status == ExecutionStatus.CLAIMED || it.status == ExecutionStatus.RUNNING) && expiresAt != null && expiresAt <= now))
            && it.scheduledAt <= now 
        }.sortedBy { it.scheduledAt }
    }

    override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
        var claimed = false
        val now = Instant.now()
        executions.computeIfPresent(id) { _, exec ->
            val expiresAt = exec.expiresAt
            val isExpired = (exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING) && expiresAt != null && expiresAt <= now
            if (exec.status == ExecutionStatus.PENDING || isExpired) {
                claimed = true
                exec.copy(
                    status = ExecutionStatus.CLAIMED,
                    workerId = workerId,
                    startedAt = now,
                    expiresAt = now.plus(leaseDuration)
                )
            } else {
                exec
            }
        }
        return claimed
    }

    override suspend fun heartbeat(id: UUID, leaseDuration: Duration): Boolean {
        var updated = false
        executions.computeIfPresent(id) { _, exec ->
            if (exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING) {
                updated = true
                exec.copy(expiresAt = Instant.now().plus(leaseDuration))
            } else {
                exec
            }
        }
        return updated
    }

    override suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID?): Boolean {
        val now = Instant.now()
        return executions.values.any { it ->
            val expiresAt = it.expiresAt
            it.id != excludeExecutionId && it.lockKey == lockKey && (it.status == ExecutionStatus.CLAIMED || it.status == ExecutionStatus.RUNNING) && (expiresAt == null || expiresAt > now)
        }
    }

    override suspend fun resetExpiredExecutions(now: Instant): Int {
        var count = 0
        executions.replaceAll { _, exec ->
            val expiresAt = exec.expiresAt
            if ((exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING) && expiresAt != null && expiresAt <= now) {
                count++
                exec.copy(
                    status = ExecutionStatus.PENDING,
                    workerId = null,
                    expiresAt = null,
                    startedAt = null
                )
            } else {
                exec
            }
        }
        return count
    }
}
