package io.khrona.core

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class RetryPolicyTest {

    @Test
    fun `calculateDelay should handle fractional factors correctly`() {
        val policy = RetryPolicy(
            maxAttempts = 5,
            initialDelay = Duration.ofSeconds(1),
            factor = 1.5,
            jitter = 0.0 // Disable jitter for deterministic testing
        )

        // Attempt 1: initialDelay * 1.5^0 = 1s
        assertEquals(Duration.ofSeconds(1), policy.calculateDelay(1))
        
        // Attempt 2: initialDelay * 1.5^1 = 1.5s
        assertEquals(Duration.ofMillis(1500), policy.calculateDelay(2))
        
        // Attempt 3: initialDelay * 1.5^2 = 2.25s
        assertEquals(Duration.ofMillis(2250), policy.calculateDelay(3))
    }

    @Test
    fun `calculateDelay should cap at maxDelay`() {
        val policy = RetryPolicy(
            maxAttempts = 5,
            initialDelay = Duration.ofSeconds(1),
            maxDelay = Duration.ofSeconds(2),
            factor = 3.0,
            jitter = 0.0
        )

        assertEquals(Duration.ofSeconds(1), policy.calculateDelay(1))
        assertEquals(Duration.ofSeconds(2), policy.calculateDelay(2)) // 1 * 3 = 3, capped at 2
        assertEquals(Duration.ofSeconds(2), policy.calculateDelay(3))
    }
}
