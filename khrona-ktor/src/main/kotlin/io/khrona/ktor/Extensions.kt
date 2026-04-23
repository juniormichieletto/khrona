package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}

fun Application.scheduler(block: KhronaConfig.() -> Unit) {
    val plugin = pluginOrNull(KhronaPlugin) ?: throw IllegalStateException("Khrona plugin not installed")
    val tempConfig = KhronaConfig()
    tempConfig.block()
    tempConfig.jobs.forEach { job ->
        plugin.scheduler.registerJob(job)
    }
}
