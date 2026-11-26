package io.khrona.core

import java.time.Duration
import java.time.Instant

enum class HealthStatus {
    STOPPED,
    STARTING,
    UP,
    DEGRADED,
    DOWN
}

data class StoreHealth(
    val status: HealthStatus,
    val latency: Duration,
    val errorType: String? = null,
    val detail: String? = null
)

data class SchedulerHealth(
    val status: HealthStatus,
    val storeHealth: StoreHealth,
    val workerId: String,
    val activeExecutions: Int,
    val lastPollAt: Instant?,
    val lastSuccessfulPollAt: Instant?,
    val consecutivePollFailures: Int,
    val lastPollErrorType: String? = null,
    val stopping: Boolean
) {
    val isHealthy: Boolean
        get() = status == HealthStatus.UP || status == HealthStatus.DEGRADED

    val isReady: Boolean
        get() = status == HealthStatus.UP
}
