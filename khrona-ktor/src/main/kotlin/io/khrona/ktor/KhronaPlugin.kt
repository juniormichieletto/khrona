package io.khrona.ktor

import io.khrona.core.KhronaConfig
import io.khrona.core.Scheduler
import io.ktor.server.application.*
import io.ktor.util.*

class KhronaPlugin(val scheduler: Scheduler) {
    companion object Plugin : BaseApplicationPlugin<Application, KhronaConfig, KhronaPlugin> {
        override val key = AttributeKey<KhronaPlugin>("Khrona")

        override fun install(pipeline: Application, configure: KhronaConfig.() -> Unit): KhronaPlugin {
            val config = KhronaConfig()
            config.configure()
            
            val scheduler = Scheduler(config, pipeline)
            
            pipeline.monitor.subscribe(ApplicationStarted) {
                scheduler.start()
            }
            
            pipeline.monitor.subscribe(ApplicationStopping) {
                kotlinx.coroutines.runBlocking {
                    scheduler.stop()
                }
            }
            
            return KhronaPlugin(scheduler)
        }
    }
}

val Khrona = KhronaPlugin
