package io.khrona.core

import java.time.Instant
import java.util.UUID

interface JobStore {
    suspend fun saveJob(job: JobDefinition)
    suspend fun getJob(jobId: String): JobDefinition?
    suspend fun listJobs(): List<JobDefinition>

    suspend fun saveExecution(execution: JobExecution)

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

    suspend fun getExecution(id: UUID): JobExecution?
    
    suspend fun listEligibleExecutions(now: Instant, limit: Int = Int.MAX_VALUE): List<JobExecution>
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
