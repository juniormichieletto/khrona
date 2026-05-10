package io.khrona.core

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneId

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
    fun `cron trigger defaults to UTC`() {
        val trigger = CronTrigger("0 9 * * *")
        val now = Instant.parse("2026-04-25T08:30:00Z")

        val next = trigger.nextExecutionTime(now)

        assertEquals("UTC", trigger.timeZone)
        assertEquals(Instant.parse("2026-04-25T09:00:00Z"), next)
    }

    @Test
    fun `cron trigger evaluates expression in configured timezone`() {
        val trigger = CronTrigger("0 9 * * *", timeZone = "America/New_York")
        val now = Instant.parse("2026-01-15T13:30:00Z")

        val next = trigger.nextExecutionTime(now)

        assertEquals(Instant.parse("2026-01-15T14:00:00Z"), next)
    }

    @Test
    fun `should fail during construction for invalid timezone`() {
        assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("0 9 * * *", timeZone = "Not/AZone")
        }
    }

    @Test
    fun `serialized cron trigger without timezone defaults to UTC`() {
        val trigger = Json.decodeFromString<CronTrigger>("""{"expression":"0 9 * * *","context":"legacy-job"}""")

        assertEquals("UTC", trigger.timeZone)
        assertEquals(Instant.parse("2026-04-25T09:00:00Z"), trigger.nextExecutionTime(Instant.parse("2026-04-25T08:30:00Z")))
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

    @Test
    fun `zone id exposes stable serialized timezone id`() {
        val trigger = CronTrigger("0 9 * * *", timeZone = ZoneId.of("Europe/London").id)

        assertEquals("Europe/London", trigger.timeZone)
    }
}
