package io.khrona.core

import java.time.Duration

class JobBuilder(val id: String) {
    var description: String? = null
    var trigger: Trigger? = null
    private var handler: JobHandler? = null
    var retryPolicy: RetryPolicy = RetryPolicy.DEFAULT
    var lockKey: String? = null
    var timeout: Duration? = null

    fun every(interval: Duration) {
        this.trigger = IntervalTrigger(interval)
    }

    fun execute(block: JobHandler) {
        this.handler = block
    }

    fun build(): JobDefinition {
        return JobDefinition(
            id = id,
            description = description,
            handler = handler ?: throw IllegalArgumentException("Handler must be defined for job $id"),
            trigger = trigger ?: throw IllegalArgumentException("Trigger must be defined for job $id"),
            retryPolicy = retryPolicy,
            lockKey = lockKey,
            timeout = timeout
        )
    }
}

class KhronaConfig {
    var store: JobStore? = null
    val jobs = mutableListOf<JobDefinition>()

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
