package io.khrona.store.memory

import io.khrona.core.ExecutionStatus
import io.khrona.core.JobDefinition
import io.khrona.core.JobExecution
import io.khrona.core.JobExecutionQuery
import io.khrona.core.JobStore
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class MemoryJobStore : JobStore {
    private val jobs = ConcurrentHashMap<String, JobDefinition>()
    private val executions = ConcurrentHashMap<UUID, JobExecution>()
    private val seededExecutionsByJob = ConcurrentHashMap<String, UUID>()

    override suspend fun saveJob(job: JobDefinition) {
        jobs[job.id] = job
    }

    override suspend fun getJob(jobId: String): JobDefinition? = jobs[jobId]

    override suspend fun listJobs(): List<JobDefinition> = jobs.values.toList()

    override suspend fun saveExecution(execution: JobExecution) {
        executions.compute(execution.id) { _, existing ->
            if (existing != null && execution.status == ExecutionStatus.PENDING && existing.status != ExecutionStatus.PENDING) {
                existing
            } else {
                execution
            }
        }
    }

    override suspend fun seedInitialExecution(execution: JobExecution): Boolean = synchronized(executions) {
        if (seededExecutionsByJob[execution.jobId] == execution.id) return false
        if (executions.containsKey(execution.id)) {
            seededExecutionsByJob[execution.jobId] = execution.id
            return false
        }
        val hasActiveOrPending = executions.values.any {
            it.jobId == execution.jobId && (
                it.status == ExecutionStatus.PENDING ||
                    it.status == ExecutionStatus.CLAIMED ||
                    it.status == ExecutionStatus.RUNNING
                )
        }
        if (hasActiveOrPending) return false
        seededExecutionsByJob[execution.jobId] = execution.id
        executions[execution.id] = execution
        return true
    }

    override suspend fun updateExecutionStatus(
        id: UUID,
        status: ExecutionStatus,
        error: String?,
        workerId: String?
    ): Boolean = completeExecution(id, status, error, workerId, null)

    override suspend fun completeExecution(
        id: UUID,
        status: ExecutionStatus,
        error: String?,
        workerId: String?,
        nextExecution: JobExecution?
    ): Boolean = synchronized(executions) {
        var updated = false
        executions.computeIfPresent(id) { _, exec ->
            if (workerId != null) {
                val isActive = exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING
                if (!isActive || exec.workerId != workerId) {
                    return@computeIfPresent exec
                }
            }
            updated = true
            exec.copy(
                status = status,
                error = error,
                workerId = if (status == ExecutionStatus.PENDING) null else exec.workerId,
                startedAt = if (status == ExecutionStatus.PENDING) null else exec.startedAt,
                expiresAt = if (status == ExecutionStatus.PENDING) null else exec.expiresAt,
                completedAt = if (status.isTerminal) Instant.now() else exec.completedAt
            )
        }
        if (updated && nextExecution != null) {
            executions.putIfAbsent(nextExecution.id, nextExecution)
        }
        return updated
    }

    override suspend fun getExecution(id: UUID): JobExecution? = executions[id]

    override suspend fun listExecutions(query: JobExecutionQuery): List<JobExecution> =
        query.selectFrom(executions.values)

    override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> {
        return listEligibleExecutions(now, limit, null)
    }

    override suspend fun listEligibleExecutions(
        now: Instant,
        limit: Int,
        jobIds: Set<String>?
    ): List<JobExecution> {
        if (limit <= 0 || (jobIds != null && jobIds.isEmpty())) return emptyList()
        return executions.values.filter { it ->
            val expiresAt = it.expiresAt
            (jobIds == null || it.jobId in jobIds) &&
                (it.status == ExecutionStatus.PENDING ||
                    ((it.status == ExecutionStatus.CLAIMED || it.status == ExecutionStatus.RUNNING) &&
                        expiresAt != null && expiresAt <= now)) &&
                it.scheduledAt <= now
        }.sortedBy { it.scheduledAt }
            .take(limit)
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

    override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
        var updated = false
        val now = Instant.now()
        executions.computeIfPresent(id) { _, exec ->
            if ((exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING) &&
                exec.workerId == workerId && exec.expiresAt?.isAfter(now) == true
            ) {
                updated = true
                exec.copy(expiresAt = now.plus(leaseDuration))
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

    override suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID?): List<UUID> {
        val superseded = mutableListOf<UUID>()
        executions.replaceAll { id, exec ->
            if (id != excludeExecutionId &&
                exec.lockKey == lockKey &&
                (exec.status == ExecutionStatus.CLAIMED || exec.status == ExecutionStatus.RUNNING)
            ) {
                superseded.add(id)
                exec.copy(status = ExecutionStatus.SUPERSEDED, completedAt = Instant.now())
            } else {
                exec
            }
        }
        return superseded
    }

    override suspend fun cleanupCompletedExecutions(
        before: Instant,
        statuses: Set<ExecutionStatus>,
        limit: Int
    ): Int {
        if (statuses.isEmpty() || limit <= 0) return 0
        val toRemove = executions.values.asSequence()
            .filter {
                val completedAt = it.completedAt
                it.status in statuses && completedAt != null && completedAt < before
            }
            .take(limit)
            .map { it.id }
            .toList()
        toRemove.forEach { executions.remove(it) }
        return toRemove.size
    }
}
