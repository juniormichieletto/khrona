package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.ktor.server.application.*

fun Application.installKhrona(configure: KhronaConfig.() -> Unit) {
    install(KhronaPlugin, configure)
}
