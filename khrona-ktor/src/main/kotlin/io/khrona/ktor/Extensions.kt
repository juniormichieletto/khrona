package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}

fun Application.scheduler(block: KhronaConfig.() -> Unit) {
    val plugin = pluginOrNull(KhronaPlugin) ?: throw IllegalStateException("Khrona plugin not installed")
    // Note: In v0.1 we use the config from the plugin installation.
    // To allow adding jobs later, we'd need to expose the config or allow dynamic registration.
    // For now, we recommend defining jobs inside install(KhronaPlugin).
}
