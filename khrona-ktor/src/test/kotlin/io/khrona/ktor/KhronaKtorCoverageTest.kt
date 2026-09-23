package io.khrona.ktor

import io.khrona.store.memory.MemoryJobStore
import io.ktor.server.application.plugin
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class KhronaKtorCoverageTest {
    @Test
    fun `installKhrona installs plugin through extension`() = testApplication {
        application {
            installKhrona {
                store = MemoryJobStore()
            }

            assertEquals(1_000, plugin(Khrona).scheduler.config.pollingInterval.toMillis())
        }
    }

    @Test
    fun `scheduler extension fails when plugin is not installed`() = testApplication {
        application {
            assertThrows(IllegalStateException::class.java) {
                runBlocking {
                    scheduler {
                        this.store = MemoryJobStore()
                    }
                }
            }
        }
    }

    @Test
    fun `scheduler extension keeps existing polling interval when block does not override it`() = testApplication {
        application {
            installKhrona {
                store = MemoryJobStore()
                pollingInterval = java.time.Duration.ofMillis(321)
            }

            runBlocking {
                scheduler {
                    job("keeps-interval") {
                        every(java.time.Duration.ofSeconds(1))
                        execute {}
                    }
                }
            }

            assertEquals(321, plugin(Khrona).scheduler.config.pollingInterval.toMillis())
        }
    }
}
