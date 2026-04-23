package io.khrona.core

import java.time.Instant
import java.util.UUID

data class JobExecution(
    val id: UUID = UUID.randomUUID(),
    val jobId: String,
    val scheduledAt: Instant,
    val startedAt: Instant? = null,
    val completedAt: Instant? = null,
    val status: ExecutionStatus = ExecutionStatus.PENDING,
    val attempt: Int = 0,
    val workerId: String? = null,
    val lockKey: String? = null,
    val error: String? = null,
    val payload: Any? = null
)
