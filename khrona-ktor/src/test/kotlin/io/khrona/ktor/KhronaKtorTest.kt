package io.khrona.ktor

import io.khrona.core.*
import io.khrona.store.memory.MemoryJobStore
import kotlinx.coroutines.runBlocking
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
            runBlocking {
                scheduler {
                    pollingInterval(1.milliseconds)
                    job("test-job") {
                        every(100.milliseconds)
                        execute {
                            println("Job executed")
                        }
                    }
                }
            }

            val plugin = plugin(Khrona)
            assertEquals(Duration.ofMillis(1), plugin.scheduler.config.pollingInterval)
            assertEquals(listOf("test-job"), plugin.scheduler.config.jobs.map { it.id })
        }
    }

    @Test
    fun `scheduler extension should support cron`() = testApplication {
        application {
            val store = MemoryJobStore()
            install(Khrona) {
                this.store = store
            }
            
            runBlocking {
                scheduler {
                    job("cron-job") {
                        cron("0 0 * * *")
                        execute {}
                    }
                }
            }

            val plugin = plugin(Khrona)
            val job = runBlocking { plugin.scheduler.config.store?.getJob("cron-job") }
            assertNotNull(job)
            assertTrue(job?.trigger is CronTrigger)
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
                runBlocking {
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

    @Test
    fun `scheduler extension propagates all configuration properties`() = testApplication {
        application {
            val store = MemoryJobStore()
            install(Khrona) {
                this.store = store
                job("initial-job") {
                    every(Duration.ofSeconds(5))
                    execute {}
                }
            }

            runBlocking {
                scheduler {
                    pollingInterval = Duration.ofMillis(250)
                    misfireThreshold = Duration.ofSeconds(15)
                    shutdownTimeout = Duration.ofSeconds(10)
                    executionLeaseDuration = Duration.ofSeconds(40)
                    heartbeatInterval = Duration.ofSeconds(10)
                    pollBatchSize = 25
                    maxConcurrentExecutions = 8
                    val customObserver = object : KhronaObserver {}
                    val customFormatter = ErrorDiagnosticFormatter { "redacted" }
                    observer = customObserver
                    errorFormatter = customFormatter
                    healthPollFailureThreshold = 4
                    deadLetterSamplingInterval = Duration.ofSeconds(45)
                    retention {
                        enabled = false
                        batchSize = 500
                    }
                    job("initial-job") {
                        description = "updated"
                        every(Duration.ofSeconds(10))
                        execute {}
                    }
                    job("second-job") {
                        every(Duration.ofSeconds(20))
                        execute {}
                    }
                }
            }

            val config = plugin(Khrona).scheduler.config
            assertEquals(Duration.ofMillis(250), config.pollingInterval)
            assertEquals(Duration.ofSeconds(15), config.misfireThreshold)
            assertEquals(Duration.ofSeconds(10), config.shutdownTimeout)
            assertEquals(Duration.ofSeconds(40), config.executionLeaseDuration)
            assertEquals(Duration.ofSeconds(10), config.heartbeatInterval)
            assertEquals(25, config.pollBatchSize)
            assertEquals(8, config.maxConcurrentExecutions)
            assertEquals(4, config.healthPollFailureThreshold)
            assertEquals(Duration.ofSeconds(45), config.deadLetterSamplingInterval)
            assertEquals("redacted", config.errorFormatter.format(IllegalStateException("secret")))
            assertNotSame(KhronaObserver.NO_OP, config.observer)
            assertFalse(config.retentionPolicy.enabled)
            assertEquals(500, config.retentionPolicy.batchSize)
            assertEquals(listOf("initial-job", "second-job"), config.jobs.map { it.id })
            assertEquals("updated", config.jobs.first { it.id == "initial-job" }.description)
        }
    }

    @Test
    fun `scheduler configuration must reject heartbeat interval beyond lease`() = testApplication {
        application {
            install(Khrona) { store = MemoryJobStore() }
            assertThrows(IllegalArgumentException::class.java) {
                runBlocking {
                    scheduler {
                        executionLeaseDuration = Duration.ofSeconds(2)
                        heartbeatInterval = Duration.ofSeconds(5)
                    }
                }
            }
        }
        startApplication()
    }

    @Test
    fun `configured replacement store must receive newly registered jobs`() = testApplication {
        application {
            val original = MemoryJobStore()
            val replacement = MemoryJobStore()
            install(Khrona) { store = original }
            runBlocking {
                scheduler {
                    store = replacement
                    job("moved") { every(Duration.ofHours(1)); execute {} }
                }
                assertNotNull(replacement.getJob("moved"), "configuration points to replacement but scheduler writes to original")
            }
        }
        startApplication()
    }
}
