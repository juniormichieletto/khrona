package io.khrona.core

import java.time.Duration
import java.time.Instant

class IntervalTrigger(
    val interval: Duration,
    val initialDelay: Duration = Duration.ZERO
) : Trigger {
    override fun nextExecutionTime(after: Instant): Instant? {
        // This is a simplified version. A real implementation might need to 
        // handle drift or fixed-rate vs fixed-delay semantics.
        return after.plus(interval)
    }
}
