package io.khrona.core

import java.time.Instant
import java.util.UUID

interface JobStore {
    suspend fun saveJob(job: JobDefinition)
    suspend fun getJob(jobId: String): JobDefinition?
    suspend fun listJobs(): List<JobDefinition>

    suspend fun saveExecution(execution: JobExecution)
    suspend fun updateExecutionStatus(id: UUID, status: ExecutionStatus, error: String? = null)
    suspend fun getExecution(id: UUID): JobExecution?
    
    suspend fun listEligibleExecutions(now: Instant): List<JobExecution>
    suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: java.time.Duration): Boolean
}
