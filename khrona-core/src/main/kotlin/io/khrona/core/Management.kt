package io.khrona.core

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.time.Instant
import java.util.UUID

data class JobExecutionQuery(
    val jobId: String? = null,
    val statuses: Set<ExecutionStatus>? = null,
    val limit: Int = DEFAULT_LIMIT,
    val scheduledFrom: Instant? = null,
    val scheduledUntil: Instant? = null
) {
    init {
        require(limit > 0) { "limit must be positive" }
        if (scheduledFrom != null && scheduledUntil != null) {
            require(!scheduledFrom.isAfter(scheduledUntil)) {
                "scheduledFrom ($scheduledFrom) must not be after scheduledUntil ($scheduledUntil)"
            }
        }
    }

    fun matches(execution: JobExecution): Boolean {
        if (jobId != null && execution.jobId != jobId) return false
        if (statuses != null && execution.status !in statuses) return false
        if (scheduledFrom != null && execution.scheduledAt.isBefore(scheduledFrom)) return false
        if (scheduledUntil != null && execution.scheduledAt.isAfter(scheduledUntil)) return false
        return true
    }

    fun selectFrom(executions: Iterable<JobExecution>): List<JobExecution> {
        return executions.asSequence()
            .filter(::matches)
            .sortedWith(NEWEST_FIRST_COMPARATOR)
            .take(limit)
            .toList()
    }

    companion object {
        const val DEFAULT_LIMIT = 100
        val NEWEST_FIRST_COMPARATOR: Comparator<JobExecution> =
            compareByDescending<JobExecution> { it.scheduledAt }.thenByDescending { it.id.toString() }
    }
}

data class JobProgress(
    val latestExecution: JobExecution?,
    val nextPendingExecution: JobExecution?,
    val activeExecutions: List<JobExecution>,
    val countsByStatus: Map<ExecutionStatus, Int>
)

data class JobOverview(
    val job: JobDefinition,
    val paused: Boolean,
    val hasLocalHandler: Boolean,
    val progress: JobProgress
)

data class JobControlResult(
    val success: Boolean,
    val affectedExecutionIds: List<UUID> = emptyList(),
    val reason: String? = null
)

class ShutdownCheckpointException(
    val executionId: UUID,
    val jobId: String
) : CancellationException("Execution $executionId for job $jobId reached shutdown checkpoint")

class ExecutionContext(
    val executionId: UUID,
    val jobId: String,
    val attempt: Int,
    val scheduledAt: Instant,
    val payload: Any?,
    val correlationId: String,
    private val isStopping: () -> Boolean
) {
    suspend fun checkpoint() {
        currentCoroutineContext().ensureActive()
        if (isStopping()) {
            throw ShutdownCheckpointException(executionId, jobId)
        }
    }
}
