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
    fun `every minute cron should trigger at the next minute`() {
        val trigger = CronTrigger("* * * * *")
        val now = Instant.parse("2026-04-25T10:00:00Z")
        
        val next = trigger.nextExecutionTime(now)
        assertEquals(Instant.parse("2026-04-25T10:01:00Z"), next)
    }

    @Test
    fun `should fail during construction for Quartz format (6 fields)`() {
        assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("0 0 * * * ?")
        }
    }

    @Test
    fun `should fail during construction for invalid cron expressions`() {
        assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("invalid cron")
        }
    }

    @Test
    fun `should include job id in error message when provided`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("invalid", "my-test-job")
        }
        assertTrue(exception.message!!.contains("Job 'my-test-job'"))
    }
}
