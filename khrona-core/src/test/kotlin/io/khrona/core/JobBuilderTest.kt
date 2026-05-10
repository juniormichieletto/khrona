package io.khrona.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.ZoneId

class JobBuilderTest {
    @Test
    fun `should default shutdown timeout below common Kubernetes grace period`() {
        val config = KhronaConfig()

        assertEquals(Duration.ofSeconds(25), config.shutdownTimeout)
    }

    @Test
    fun `should default lockKey to id when policy is FORBID`() {
        val job = JobBuilder("test-job").apply {
            concurrencyPolicy = ConcurrencyPolicy.FORBID
            every(Duration.ofMinutes(1))
            execute {}
        }.build()
        
        assertEquals("test-job", job.lockKey)
    }

    @Test
    fun `should default lockKey to id when policy is REPLACE`() {
        val job = JobBuilder("test-job").apply {
            concurrencyPolicy = ConcurrencyPolicy.REPLACE
            every(Duration.ofMinutes(1))
            execute {}
        }.build()
        
        assertEquals("test-job", job.lockKey)
    }

    @Test
    fun `should not default lockKey when policy is ALLOW`() {
        val job = JobBuilder("test-job").apply {
            concurrencyPolicy = ConcurrencyPolicy.ALLOW
            every(Duration.ofMinutes(1))
            execute {}
        }.build()
        
        assertNull(job.lockKey)
    }

    @Test
    fun `should allow explicit lockKey override`() {
        val job = JobBuilder("test-job").apply {
            concurrencyPolicy = ConcurrencyPolicy.FORBID
            lockKey = "custom-lock"
            every(Duration.ofMinutes(1))
            execute {}
        }.build()
        
        assertEquals("custom-lock", job.lockKey)
    }

    @Test
    fun `cron DSL should support zone id`() {
        val job = JobBuilder("test-job").apply {
            cron("0 9 * * *", ZoneId.of("America/New_York"))
            execute {}
        }.build()

        assertTrue(job.trigger is CronTrigger)
        assertEquals("America/New_York", (job.trigger as CronTrigger).timeZone)
    }

    @Test
    fun `should fail to build job without trigger`() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java) {
            JobBuilder("test").apply {
                execute {}
            }.build()
        }
    }

    @Test
    fun `should fail to build job with invalid retry policy`() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java) {
            JobBuilder("test").apply {
                retry { maxAttempts = 0 }
                every(Duration.ofMinutes(1))
                execute {}
            }.build()
        }

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java) {
            JobBuilder("test").apply {
                retry { factor = 0.5 } // Must be >= 1.0
                every(Duration.ofMinutes(1))
                execute {}
            }.build()
        }

        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException::class.java) {
            JobBuilder("test").apply {
                retry { jitter = 1.5 } // Must be 0..1
                every(Duration.ofMinutes(1))
                execute {}
            }.build()
        }
    }
}
