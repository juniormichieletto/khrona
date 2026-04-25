package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}

fun Application.scheduler(block: KhronaConfig.() -> Unit) {
    val plugin = pluginOrNull(KhronaPlugin) ?: throw IllegalStateException("Khrona plugin not installed")
    val tempConfig = KhronaConfig()
    // Initialize with current value
    tempConfig.pollingInterval = plugin.scheduler.config.pollingInterval
    
    tempConfig.block()
    
    // Apply potentially updated polling interval
    plugin.scheduler.config.pollingInterval = tempConfig.pollingInterval
    
    tempConfig.jobs.forEach { job ->
        plugin.scheduler.registerJob(job)
    }
}
