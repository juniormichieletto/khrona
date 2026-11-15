package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}

suspend fun Application.scheduler(block: KhronaConfig.() -> Unit) {
    val plugin = pluginOrNull(KhronaPlugin) ?: throw IllegalStateException("Khrona plugin not installed")
    val config = plugin.scheduler.config
    val existingJobs = config.jobs.toList()
    config.jobs.clear()

    config.block()

    val newJobs = config.jobs.toList()
    config.jobs.clear()
    config.jobs.addAll(existingJobs)

    newJobs.forEach { job ->
        plugin.scheduler.registerJob(job)
        config.jobs.removeAll { it.id == job.id }
        config.jobs.add(job)
    }
}
