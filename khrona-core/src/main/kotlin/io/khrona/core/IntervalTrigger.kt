package io.khrona.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.time.Duration
import java.time.Instant

@Serializable
@SerialName("interval")
class IntervalTrigger(
    @Serializable(with = DurationSerializer::class)
    val interval: Duration,
    @Serializable(with = DurationSerializer::class)
    val initialDelay: Duration = Duration.ZERO
) : Trigger {
    constructor(
        interval: kotlin.time.Duration,
        initialDelay: kotlin.time.Duration = kotlin.time.Duration.ZERO
    ) : this(
        interval = java.time.Duration.ofMillis(interval.inWholeMilliseconds),
        initialDelay = java.time.Duration.ofMillis(initialDelay.inWholeMilliseconds)
    )

    override fun nextExecutionTime(after: Instant): Instant? {
        // This is a simplified version. A real implementation might need to 
        // handle drift or fixed-rate vs fixed-delay semantics.
        return after.plus(interval)
    }
}
