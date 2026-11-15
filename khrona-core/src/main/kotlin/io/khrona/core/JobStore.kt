package io.khrona.core

import java.time.Instant
import java.util.UUID

interface JobStore {
    suspend fun saveJob(job: JobDefinition)
    suspend fun getJob(jobId: String): JobDefinition?
    suspend fun listJobs(): List<JobDefinition>

    suspend fun saveExecution(execution: JobExecution)

    /**
     * Atomically seeds the initial scheduled execution for a job only if no execution with
     * [execution.id] already exists and no non-terminal (`PENDING`, `CLAIMED`, or `RUNNING`)
     * execution currently exists for [execution.jobId].
     *
     * Returns `true` if [execution] was persisted, or `false` if seeding was skipped.
     */
    suspend fun seedInitialExecution(execution: JobExecution): Boolean {
        if (getExecution(execution.id) != null) return false
        saveExecution(execution)
        return true
    }

    /**
     * Updates the status of execution [id] to [status].
     *
     * When [workerId] is non-null, the store must atomically verify that the execution is currently
     * in an active (`CLAIMED` or `RUNNING`) state and owned by [workerId] before applying the transition.
     * When transitioning to `PENDING` (releasing a claim), the store clears `workerId`, `startedAt`, and `expiresAt`.
     * Returns `true` if the execution was updated, or `false` if the execution does not exist or ownership/state validation failed.
     */
    suspend fun updateExecutionStatus(
        id: UUID,
        status: ExecutionStatus,
        error: String? = null,
        workerId: String? = null
    ): Boolean

    /**
     * Atomically updates the status of execution [id] to [status] and persists [nextExecution]
     * (such as a recurring follow-up or retry execution) when non-null and not already present.
     *
     * When [workerId] is non-null, the store verifies that [id] is currently in an active
     * (`CLAIMED` or `RUNNING`) state and owned by [workerId] before applying the transition or
     * persisting [nextExecution].
     */
    suspend fun completeExecution(
        id: UUID,
        status: ExecutionStatus,
        error: String? = null,
        workerId: String? = null,
        nextExecution: JobExecution? = null
    ): Boolean {
        val current = getExecution(id) ?: return false
        if (workerId != null) {
            val isActive = current.status == ExecutionStatus.CLAIMED || current.status == ExecutionStatus.RUNNING
            if (!isActive || current.workerId != workerId) return false
        }
        if (nextExecution != null && getExecution(nextExecution.id) == null) {
            saveExecution(nextExecution)
        }
        return updateExecutionStatus(id, status, error, workerId)
    }

    suspend fun getExecution(id: UUID): JobExecution?
    
    suspend fun listEligibleExecutions(now: Instant, limit: Int = Int.MAX_VALUE): List<JobExecution>

    suspend fun listEligibleExecutions(
        now: Instant,
        limit: Int = Int.MAX_VALUE,
        jobIds: Set<String>?
    ): List<JobExecution> {
        if (limit <= 0) return emptyList()
        val eligible = listEligibleExecutions(now, if (jobIds == null) limit else Int.MAX_VALUE)
        if (jobIds == null) return eligible.take(limit)
        if (jobIds.isEmpty()) return emptyList()
        return eligible.asSequence().filter { it.jobId in jobIds }.take(limit).toList()
    }

    suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: java.time.Duration): Boolean
    
    /**
     * Atomically renews an unexpired CLAIMED/RUNNING execution owned by [workerId].
     * Returns false without changing the execution when ownership or lease validation fails,
     * including when the lease expired before any worker reclaimed it.
     * The scheduler requests cooperative cancellation when renewal returns false.
     */
    suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: java.time.Duration): Boolean
    suspend fun isLockHeld(lockKey: String, excludeExecutionId: UUID? = null): Boolean
    suspend fun resetExpiredExecutions(now: Instant): Int

    /**
     * Marks all RUNNING/CLAIMED executions for a specific lock key as SUPERSEDED.
     * Returns the list of UUIDs that were superseded.
     */
    suspend fun supersedeExecutionsByLockKey(lockKey: String, excludeExecutionId: UUID? = null): List<UUID>

    /**
     * Purges terminal executions completed before [before] matching [statuses], up to [limit] rows.
     * Returns the number of deleted executions.
     */
    suspend fun cleanupCompletedExecutions(
        before: Instant,
        statuses: Set<ExecutionStatus> = setOf(
            ExecutionStatus.SUCCESS,
            ExecutionStatus.FAILED,
            ExecutionStatus.MISFIRED,
            ExecutionStatus.SUPERSEDED
        ),
        limit: Int = 1000
    ): Int
}
