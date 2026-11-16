@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.*
import java.util.UUID

class SchedulerLeaseSafetyTest {
    class TestClock(private val time: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(time.currentTime)
    }
    @Test
    fun `delayed successful renewal must not extend local deadline beyond stored lease`() = runTest {
        val clock = TestClock(testScheduler)
        var renewals = 0
        val store = object : MockJobStore(clock) {
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
                if (++renewals > 1) error("renewals unavailable")
                val ok = super.heartbeat(id, workerId, leaseDuration)
                delay(500) // persisted renewal succeeds, response takes time to reach worker
                return ok
            }
        }
        var cancelled = false
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofSeconds(2)
            heartbeatInterval = Duration.ofMillis(500)
            job("busy") { every(Duration.ofHours(1)); execute { try { awaitCancellation() } finally { cancelled = true } } }
        }
        config.jobs.forEach { store.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        store.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent(); advanceTimeBy(2750); runCurrent()
        assertTrue(store.executions[execution.id]!!.expiresAt!! < clock.instant(), "stored lease must have expired")
        assertTrue(store.claimExecution(execution.id,"replacement", Duration.ofSeconds(2)), "replacement must be able to reclaim")
        assertTrue(cancelled,"old handler still running after replacement reclaimed execution")
    }


    @Test
    fun `delayed claim response does not extend the initial lease deadline`() = runTest {
        val clock = TestClock(testScheduler)
        val store = object : MockJobStore(clock) {
            override suspend fun claimExecution(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
                val claimed = super.claimExecution(id, workerId, leaseDuration)
                delay(500)
                return claimed
            }
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean = error("unavailable")
        }
        var cancelled = false
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofSeconds(2)
            heartbeatInterval = Duration.ofMillis(500)
            job("busy") { every(Duration.ofHours(1)); execute { try { awaitCancellation() } finally { cancelled = true } } }
        }
        config.jobs.forEach { store.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        store.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        advanceTimeBy(1750); runCurrent()
        assertTrue(store.executions[execution.id]!!.expiresAt!! < clock.instant())
        assertTrue(cancelled, "initial claim response latency must not extend ownership")
    }

    @Test
    fun `watchdog expires even when wall clock stops advancing`() = runBlocking {
        val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
        val store = object : MockJobStore(clock) {
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean = error("unavailable")
        }
        val entered = CompletableDeferred<Unit>()
        val cancelled = CompletableDeferred<Unit>()
        val workerScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofMillis(100)
            heartbeatInterval = Duration.ofMillis(20)
            job("busy") { every(Duration.ofHours(1)); execute {
                entered.complete(Unit)
                try { awaitCancellation() } finally { cancelled.complete(Unit) }
            } }
        }
        config.jobs.forEach { store.saveJob(it) }
        store.saveExecution(JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH))
        try {
            Scheduler(config, workerScope, clock).pollAndExecute()
            withTimeout(2000) { entered.await(); cancelled.await() }
        } finally { workerScope.cancel() }
    }

    @Test
    fun `transient heartbeat failure inside the lease budget does not cancel the handler`() = runTest {
        val clock = TestClock(testScheduler)
        var calls = 0
        val store = object : MockJobStore(clock) {
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
                if (++calls == 1) error("temporary renewal failure")
                return super.heartbeat(id, workerId, leaseDuration)
            }
        }
        var cancelled = false
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofSeconds(2)
            heartbeatInterval = Duration.ofMillis(500)
            job("busy") { every(Duration.ofHours(1)); execute { try { awaitCancellation() } finally { cancelled = true } } }
        }
        config.jobs.forEach { store.saveJob(it) }
        val execution = JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH)
        store.saveExecution(execution)
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        advanceTimeBy(501); runCurrent()
        assertFalse(cancelled, "a transient renewal error must not cancel a still-valid claim")
        advanceTimeBy(1000); runCurrent()
        assertFalse(cancelled, "successful renewals must keep the handler running")
        assertTrue(store.executions[execution.id]!!.expiresAt!! > clock.instant())
    }

    @Test
    fun `watchdog cancels at the exact lease deadline`() = runTest {
        val clock = TestClock(testScheduler)
        val store = object : MockJobStore(clock) {
            override suspend fun heartbeat(id: UUID, workerId: String, leaseDuration: Duration): Boolean {
                delay(10_000)
                return false
            }
        }
        var cancelled = false
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofSeconds(2)
            heartbeatInterval = Duration.ofMillis(500)
            job("busy") { every(Duration.ofHours(1)); execute { try { awaitCancellation() } finally { cancelled = true } } }
        }
        config.jobs.forEach { store.saveJob(it) }
        store.saveExecution(JobExecution(jobId = "busy", scheduledAt = Instant.EPOCH))
        val scheduler = Scheduler(config, backgroundScope, clock)
        scheduler.pollAndExecute(); runCurrent()
        advanceTimeBy(1999); runCurrent()
        assertFalse(cancelled)
        advanceTimeBy(1); runCurrent()
        assertTrue(cancelled, "ownership ends at expiry, not one tick later")
    }
}
