package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}

suspend fun Application.scheduler(block: KhronaConfig.() -> Unit) {
    val plugin = pluginOrNull(KhronaPlugin) ?: throw IllegalStateException("Khrona plugin not installed")
    val currentConfig = plugin.scheduler.config
    val candidate = KhronaConfig().apply {
        store = currentConfig.store
        pollingInterval = currentConfig.pollingInterval
        misfireThreshold = currentConfig.misfireThreshold
        shutdownTimeout = currentConfig.shutdownTimeout
        executionLeaseDuration = currentConfig.executionLeaseDuration
        heartbeatInterval = currentConfig.heartbeatInterval
        pollBatchSize = currentConfig.pollBatchSize
        maxConcurrentExecutions = currentConfig.maxConcurrentExecutions
        retentionPolicy.enabled = currentConfig.retentionPolicy.enabled
        retentionPolicy.maxAge = currentConfig.retentionPolicy.maxAge
        retentionPolicy.cleanupInterval = currentConfig.retentionPolicy.cleanupInterval
        retentionPolicy.cleanupJitter = currentConfig.retentionPolicy.cleanupJitter
        retentionPolicy.batchSize = currentConfig.retentionPolicy.batchSize
        retentionPolicy.statuses = currentConfig.retentionPolicy.statuses
    }
    candidate.block()
    candidate.validate()

    currentConfig.store = candidate.store
    currentConfig.pollingInterval = candidate.pollingInterval
    currentConfig.misfireThreshold = candidate.misfireThreshold
    currentConfig.shutdownTimeout = candidate.shutdownTimeout
    currentConfig.executionLeaseDuration = candidate.executionLeaseDuration
    currentConfig.heartbeatInterval = candidate.heartbeatInterval
    currentConfig.pollBatchSize = candidate.pollBatchSize
    currentConfig.maxConcurrentExecutions = candidate.maxConcurrentExecutions
    currentConfig.retentionPolicy.enabled = candidate.retentionPolicy.enabled
    currentConfig.retentionPolicy.maxAge = candidate.retentionPolicy.maxAge
    currentConfig.retentionPolicy.cleanupInterval = candidate.retentionPolicy.cleanupInterval
    currentConfig.retentionPolicy.cleanupJitter = candidate.retentionPolicy.cleanupJitter
    currentConfig.retentionPolicy.batchSize = candidate.retentionPolicy.batchSize
    currentConfig.retentionPolicy.statuses = candidate.retentionPolicy.statuses

    candidate.jobs.forEach { job ->
        plugin.scheduler.registerJob(job)
        currentConfig.jobs.removeAll { it.id == job.id }
        currentConfig.jobs.add(job)
    }
}
