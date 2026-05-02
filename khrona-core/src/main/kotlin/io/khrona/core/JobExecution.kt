package io.khrona.core

import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import java.time.Instant
import java.util.UUID

@Serializable
data class JobExecution(
    val id: @Serializable(with = UUIDSerializer::class) UUID = UUID.randomUUID(),
    val jobId: String,
    val scheduledAt: @Serializable(with = InstantSerializer::class) Instant,
    val startedAt: @Serializable(with = InstantSerializer::class) Instant? = null,
    val completedAt: @Serializable(with = InstantSerializer::class) Instant? = null,
    val expiresAt: @Serializable(with = InstantSerializer::class) Instant? = null,
    val status: ExecutionStatus = ExecutionStatus.PENDING,
    val attempt: Int = 0,
    val workerId: String? = null,
    val lockKey: String? = null,
    val error: String? = null,
    @Contextual
    val payload: Any? = null,
    val correlationId: String? = id.toString()
)
