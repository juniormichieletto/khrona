package io.khrona.core

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

class CronTriggerTest {
    @Test
    fun `should support Unix cron format (5 fields)`() {
        val trigger = CronTrigger("*/5 * * * *") // Every 5 minutes
        val now = Instant.parse("2026-04-25T10:00:00Z")
        
        val next = trigger.nextExecutionTime(now)
        assertEquals(Instant.parse("2026-04-25T10:05:00Z"), next)
    }

    @Test
    fun `should support Quartz cron format (6 fields)`() {
        val trigger = CronTrigger("0 0 * * * ?") // Every hour
        val now = Instant.parse("2026-04-25T10:15:00Z")
        
        val next = trigger.nextExecutionTime(now)
        assertEquals(Instant.parse("2026-04-25T11:00:00Z"), next)
    }

    @Test
    fun `Quartz cron with star in first field should trigger every second`() {
        val trigger = CronTrigger("* * * * * ?")
        val now = Instant.parse("2026-04-25T10:00:00Z")
        
        val next = trigger.nextExecutionTime(now)
        assertEquals(Instant.parse("2026-04-25T10:00:01Z"), next)
    }

    @Test
    fun `should handle invalid cron expressions gracefully`() {
        val trigger = CronTrigger("invalid cron")
        val now = Instant.now()
        
        assertNull(trigger.nextExecutionTime(now))
    }
}
