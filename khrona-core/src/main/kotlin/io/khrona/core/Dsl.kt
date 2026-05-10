package io.khrona.core

import java.time.Duration
import java.time.ZoneId

class JobBuilder(val id: String) {
    var description: String? = null
    var trigger: Trigger? = null
    private var handler: JobHandler? = null
    var retryPolicy: RetryPolicy = RetryPolicy.DEFAULT
    var concurrencyPolicy: ConcurrencyPolicy = ConcurrencyPolicy.FORBID
    var misfirePolicy: MisfirePolicy = MisfirePolicy.FIRE_NOW
    var lockKey: String? = null
    var timeout: Duration? = null

    fun retry(block: RetryPolicyBuilder.() -> Unit) {
        val builder = RetryPolicyBuilder()
        builder.block()
        this.retryPolicy = builder.build()
    }

    fun every(interval: java.time.Duration) {
        if (this.trigger != null) throw IllegalStateException("Trigger already defined for job $id")
        this.trigger = IntervalTrigger(interval)
    }

    /**
     * Extension to support kotlin.time.Duration in the DSL
     */
    fun every(interval: kotlin.time.Duration) {
        every(java.time.Duration.ofMillis(interval.inWholeMilliseconds))
    }

    /**
     * Schedules the job to run once as soon as the scheduler starts.
     */
    fun once() {
        if (this.trigger != null) throw IllegalStateException("Trigger already defined for job $id")
        this.trigger = OneTimeTrigger(java.time.Instant.EPOCH)
    }

    /**
     * Schedules the job to run once at the specified [time].
     */
    fun at(time: java.time.Instant) {
        if (this.trigger != null) throw IllegalStateException("Trigger already defined for job $id")
        this.trigger = OneTimeTrigger(time)
    }

    /**
     * Defines a cron trigger using Unix format (5 fields: min, hour, dom, month, dow).
     *
     * Example for every minute: "* * * * *"
     */
    fun cron(expression: String) {
        if (this.trigger != null) throw IllegalStateException("Trigger already defined for job $id")
        this.trigger = CronTrigger(expression, id)
    }

    /**
     * Defines a cron trigger evaluated in the provided [zoneId].
     */
    fun cron(expression: String, zoneId: ZoneId) {
        cron(expression, zoneId.id)
    }

    /**
     * Defines a cron trigger evaluated in the provided IANA timezone ID.
     */
    fun cron(expression: String, timeZone: String) {
        if (this.trigger != null) throw IllegalStateException("Trigger already defined for job $id")
        this.trigger = CronTrigger(expression, id, timeZone)
    }

    fun execute(block: JobHandler) {
        this.handler = block
    }

    fun build(): JobDefinition {
        val finalLockKey = lockKey ?: if (concurrencyPolicy != ConcurrencyPolicy.ALLOW) id else null
        return JobDefinition(
            id = id,
            description = description,
            handler = handler ?: throw IllegalArgumentException("Handler must be defined for job $id"),
            trigger = trigger ?: throw IllegalArgumentException("Trigger must be defined for job $id"),
            retryPolicy = retryPolicy,
            concurrencyPolicy = concurrencyPolicy,
            misfirePolicy = misfirePolicy,
            lockKey = finalLockKey,
            timeout = timeout
        )
    }
}

class RetryPolicyBuilder {
    var maxAttempts: Int = 3
    var initialDelay: java.time.Duration = java.time.Duration.ofSeconds(1)
    var maxDelay: java.time.Duration = java.time.Duration.ofMinutes(5)
    var factor: Double = 2.0
    var jitter: Double = 0.1

    fun build(): RetryPolicy {
        return RetryPolicy(
            maxAttempts = maxAttempts,
            initialDelay = initialDelay,
            maxDelay = maxDelay,
            factor = factor,
            jitter = jitter
        )
    }
}

class KhronaConfig {
    var store: JobStore? = null
    var pollingInterval: java.time.Duration = java.time.Duration.ofMillis(1000)
    var misfireThreshold: java.time.Duration = java.time.Duration.ofSeconds(60)
    var shutdownTimeout: java.time.Duration = java.time.Duration.ofSeconds(25)
    var executionLeaseDuration: java.time.Duration = java.time.Duration.ofMinutes(5)
    var heartbeatInterval: java.time.Duration? = null
    var pollBatchSize: Int = 100
    val jobs = mutableListOf<JobDefinition>()

    fun pollingInterval(interval: kotlin.time.Duration) {
        this.pollingInterval = java.time.Duration.ofMillis(interval.inWholeMilliseconds)
    }

    fun misfireThreshold(interval: kotlin.time.Duration) {
        this.misfireThreshold = java.time.Duration.ofMillis(interval.inWholeMilliseconds)
    }

    fun shutdownTimeout(timeout: kotlin.time.Duration) {
        this.shutdownTimeout = java.time.Duration.ofMillis(timeout.inWholeMilliseconds)
    }

    fun executionLeaseDuration(duration: kotlin.time.Duration) {
        this.executionLeaseDuration = java.time.Duration.ofMillis(duration.inWholeMilliseconds)
    }

    fun heartbeatInterval(interval: kotlin.time.Duration) {
        this.heartbeatInterval = java.time.Duration.ofMillis(interval.inWholeMilliseconds)
    }

    fun job(id: String, block: JobBuilder.() -> Unit) {
        val builder = JobBuilder(id)
        builder.block()
        jobs.add(builder.build())
    }
}

fun Khrona(block: KhronaConfig.() -> Unit): KhronaConfig {
    val config = KhronaConfig()
    config.block()
    return config
}
