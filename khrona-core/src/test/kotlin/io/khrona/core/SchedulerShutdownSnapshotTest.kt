@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class SchedulerShutdownSnapshotTest {
    private class ShrinkingClaims : ConcurrentHashMap<UUID, Any>() {
        override val size: Int
            get() {
                val observed = super.size
                // Model the last handler removing its claim after the snapshot observes size.
                clear()
                return observed
            }
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun `shutdown tolerates the last claim completing during its snapshot`() = runTest {
        val store = MockJobStore()
        val config = KhronaConfig().apply {
            this.store = store
            shutdownTimeout = Duration.ofMillis(100)
            job("snapshot") {
                every(Duration.ofHours(1))
                execute { awaitCancellation() }
            }
        }
        config.jobs.forEach { store.saveJob(it) }
        store.saveExecution(JobExecution(jobId = "snapshot", scheduledAt = Instant.EPOCH))
        val scheduler = Scheduler(config, backgroundScope)
        scheduler.pollAndExecute()
        runCurrent()

        val field = Scheduler::class.java.getDeclaredField("activeClaims").apply { isAccessible = true }
        val claims = field.get(scheduler) as ConcurrentHashMap<UUID, Any>
        assertEquals(1, claims.size, "The race requires exactly one remaining claim")
        val controlledClaims = ShrinkingClaims().apply { putAll(claims) }
        field.set(scheduler, controlledClaims)

        scheduler.stop()
    }
}
