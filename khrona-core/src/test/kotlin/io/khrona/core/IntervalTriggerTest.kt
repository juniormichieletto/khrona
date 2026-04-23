package io.khrona.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant

class IntervalTriggerTest {

    @Test
    fun `should calculate next execution time correctly`() {
        val trigger = IntervalTrigger(Duration.ofMinutes(10))
        val now = Instant.parse("2026-04-23T10:00:00Z")
        
        val next = trigger.nextExecutionTime(now)
        
        assertEquals(Instant.parse("2026-04-23T10:10:00Z"), next)
    }
}
