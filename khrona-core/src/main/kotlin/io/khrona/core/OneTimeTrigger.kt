package io.khrona.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.time.Instant

/**
 * A trigger that fires only once at a specific [executionTime].
 */
@Serializable
@SerialName("once")
data class OneTimeTrigger(
    @Serializable(with = InstantSerializer::class)
    val executionTime: Instant
) : Trigger {
    override fun nextExecutionTime(after: Instant): Instant? {
        return if (!after.isAfter(executionTime)) executionTime else null
    }
}
