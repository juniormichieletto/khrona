@file:OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)

package io.khrona.core

import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Stream

class SchedulerPollingIntervalTest {

    @ParameterizedTest(name = "single instance with polling interval {0}")
    @MethodSource("pollingIntervals")
    fun `single scheduler should run each trigger type once per scheduled time`(pollingInterval: Duration) = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val counts = ConcurrentHashMap<String, AtomicInteger>()
        val config = pollingIntervalConfig(store, pollingInterval, counts)
        val scheduler = Scheduler(config, backgroundScope, clock)

        scheduler.start()
        advanceTimeBy(Duration.ofSeconds(90).toMillis())

        assertEquals(1, counts["one-time"]?.get(), "One-time job should run once")
        assertEquals(1, counts["interval"]?.get(), "Interval job should run the first due occurrence once")
        assertEquals(1, counts["cron"]?.get(), "Cron job should run the first due occurrence once")
        assertNoDuplicateSuccessfulExecutions(store)

        scheduler.stop()
    }

    @ParameterizedTest(name = "multi instance with polling interval {0}")
    @MethodSource("pollingIntervals")
    fun `multiple schedulers should not duplicate trigger executions for each scheduled time`(pollingInterval: Duration) = runTest {
        val clock = SchedulerTest.TestClock(testScheduler)
        val store = MockJobStore(clock)
        val counts = ConcurrentHashMap<String, AtomicInteger>()
        val schedulers = (1..3).map {
            Scheduler(pollingIntervalConfig(store, pollingInterval, counts), backgroundScope, clock)
        }

        schedulers.forEach { it.start() }
        advanceTimeBy(Duration.ofSeconds(90).toMillis())

        assertEquals(1, counts["one-time"]?.get(), "One-time job should run once across all instances")
        assertEquals(1, counts["interval"]?.get(), "Interval job should run the first due occurrence once across all instances")
        assertEquals(1, counts["cron"]?.get(), "Cron job should run the first due occurrence once across all instances")
        assertNoDuplicateSuccessfulExecutions(store)

        schedulers.forEach { it.stop() }
    }

    private fun TestScope.pollingIntervalConfig(
        store: MockJobStore,
        pollingInterval: Duration,
        counts: ConcurrentHashMap<String, AtomicInteger>
    ): KhronaConfig = KhronaConfig().apply {
        this.store = store
        this.pollingInterval = pollingInterval

        job("one-time") {
            once()
            execute { counts.computeIfAbsent("one-time") { AtomicInteger(0) }.incrementAndGet() }
        }

        job("interval") {
            every(Duration.ofMinutes(1))
            execute { counts.computeIfAbsent("interval") { AtomicInteger(0) }.incrementAndGet() }
        }

        job("cron") {
            cron("* * * * *")
            execute { counts.computeIfAbsent("cron") { AtomicInteger(0) }.incrementAndGet() }
        }
    }

    private fun assertNoDuplicateSuccessfulExecutions(store: MockJobStore) {
        val duplicates = store.executions.values
            .filter { it.status == ExecutionStatus.SUCCESS }
            .groupBy { it.jobId to it.scheduledAt }
            .filterValues { it.size > 1 }

        assertTrue(duplicates.isEmpty(), "No job should run twice for the same scheduled time: $duplicates")
    }

    companion object {
        @JvmStatic
        fun pollingIntervals(): Stream<Duration> = Stream.of(
            Duration.ofMillis(100),
            Duration.ofMillis(500),
            Duration.ofSeconds(10),
            Duration.ofMinutes(1)
        )
    }
}
