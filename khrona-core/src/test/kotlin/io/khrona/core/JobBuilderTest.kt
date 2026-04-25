package io.khrona.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Duration

class JobBuilderTest {
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
}
