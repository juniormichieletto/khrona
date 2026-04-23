package io.khrona.core

import java.time.Duration
import java.time.Instant

typealias JobHandler = suspend (payload: Any?) -> Unit

data class JobDefinition(
    val id: String,
    val description: String? = null,
    val handler: JobHandler,
    val trigger: Trigger,
    val retryPolicy: RetryPolicy = RetryPolicy.DEFAULT,
    val lockKey: String? = null,
    val timeout: Duration? = null
)

interface Trigger {
    fun nextExecutionTime(after: Instant): Instant?
}

data class RetryPolicy(
    val maxAttempts: Int = 3,
    val initialDelay: Duration = Duration.ofSeconds(1),
    val maxDelay: Duration = Duration.ofMinutes(5),
    val factor: Double = 2.0
) {
    companion object {
        val DEFAULT = RetryPolicy()
    }
}
