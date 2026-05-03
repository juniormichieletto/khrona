@file:OptIn(ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

class LeaseConfigurationTest {

    class TestClock(private val scheduler: TestCoroutineScheduler) : Clock() {
        override fun getZone(): ZoneId = ZoneId.of("UTC")
        override fun withZone(zone: ZoneId?): Clock = this
        override fun instant(): Instant = Instant.ofEpochMilli(scheduler.currentTime)
    }

    @Test
    fun `scheduler should use configured lease duration`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            // Task 3.2: will add these properties
            executionLeaseDuration = Duration.ofMinutes(10)
            job("lease-test-job") {
                every(Duration.ofHours(1))
                execute { delay(Duration.ofMinutes(15).toMillis()) } // Run longer than lease
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        val jobDef = config.jobs.first()
        val execution = JobExecution(jobId = jobDef.id, scheduledAt = Instant.now(clock))
        store.saveExecution(execution)
        store.saveJob(jobDef)

        val startTime = Instant.now(clock)
        scheduler.start()
        
        // Wait for poll
        advanceTimeBy(1100)
        
        val claimed = store.getExecution(execution.id)
        assertTrue(claimed?.status == ExecutionStatus.CLAIMED || claimed?.status == ExecutionStatus.RUNNING)
        
        // Check lease expiry: startTime + 10m (since poll happened at T=0)
        assertEquals(startTime.plus(Duration.ofMinutes(10)), claimed?.expiresAt)
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should use configured heartbeat interval`() = runTest {
        val clock = TestClock(testScheduler)
        val store = MockJobStore(clock)
        val config = KhronaConfig().apply {
            this.store = store
            executionLeaseDuration = Duration.ofMinutes(10)
            heartbeatInterval = Duration.ofMinutes(2)
            job("heartbeat-test-job") {
                every(Duration.ofHours(1))
                execute { delay(Duration.ofMinutes(15).toMillis()) }
            }
        }
        
        val scheduler = Scheduler(config, backgroundScope, clock)
        val jobDef = config.jobs.first()
        val execution = JobExecution(jobId = jobDef.id, scheduledAt = Instant.now(clock))
        store.saveExecution(execution)
        store.saveJob(jobDef)

        scheduler.start()
        advanceTimeBy(1100) // Initial claim
        
        val firstExpiry = store.getExecution(execution.id)?.expiresAt
        assertNotNull(firstExpiry)
        
        // Wait for first heartbeat at 2m
        advanceTimeBy(Duration.ofMinutes(2).toMillis() + 100)
        
        val secondExpiry = store.getExecution(execution.id)?.expiresAt
        assertNotNull(secondExpiry)
        assertTrue(secondExpiry!! > firstExpiry!!, "Expiry should have been extended by heartbeat")
        
        scheduler.stop()
    }

    @Test
    fun `scheduler should fail to start with invalid lease configuration`() = runTest {
        val store = MockJobStore()
        
        // Case 1: Lease <= 0
        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                executionLeaseDuration = Duration.ZERO
            }
            Scheduler(config, this)
        }

        // Case 2: Heartbeat <= 0
        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                heartbeatInterval = Duration.ofSeconds(-1)
            }
            Scheduler(config, this)
        }

        // Case 3: Heartbeat >= Lease
        assertThrows(IllegalArgumentException::class.java) {
            val config = KhronaConfig().apply {
                this.store = store
                executionLeaseDuration = Duration.ofMinutes(5)
                heartbeatInterval = Duration.ofMinutes(5)
            }
            Scheduler(config, this)
        }
    }
}
