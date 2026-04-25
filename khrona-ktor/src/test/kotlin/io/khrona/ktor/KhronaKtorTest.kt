package io.khrona.ktor

import io.khrona.store.memory.MemoryJobStore
import io.ktor.server.application.*
import io.ktor.server.testing.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import kotlin.time.Duration.Companion.milliseconds
import java.time.Duration

class KhronaKtorTest {
    @Test
    fun `scheduler extension should respect pollingInterval`() = testApplication {
        application {
            val store = MemoryJobStore()
            install(Khrona) {
                this.store = store
            }
            
            // This should not throw Exception now
            scheduler {
                pollingInterval(1.milliseconds)
                job("test-job") {
                    every(100.milliseconds)
                    execute {
                        println("Job executed")
                    }
                }
            }

            val plugin = plugin(Khrona)
            assertEquals(Duration.ofMillis(1), plugin.scheduler.config.pollingInterval)
        }
    }

    @Test
    fun `scheduler extension should still validate job interval`() = testApplication {
        application {
            val store = MemoryJobStore()
            install(Khrona) {
                this.store = store
                pollingInterval = Duration.ofSeconds(1)
            }
            
            assertThrows(IllegalArgumentException::class.java) {
                scheduler {
                    // Not setting pollingInterval here, so it's still 1s
                    job("invalid-job") {
                        every(100.milliseconds) // 100ms < 1s
                        execute {}
                    }
                }
            }
        }
    }
}
