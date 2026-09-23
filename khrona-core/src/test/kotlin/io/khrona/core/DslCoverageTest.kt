package io.khrona.core

import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class DslCoverageTest {
    @Test
    fun `Khrona DSL applies duration helpers and creates jobs`() {
        val config = Khrona {
            pollingInterval(250.milliseconds)
            misfireThreshold(500.milliseconds)
            shutdownTimeout(750.milliseconds)
            executionLeaseDuration(1_000.milliseconds)
            heartbeatInterval(250.milliseconds)
            job("dsl-job") {
                description = "covered"
                retry {
                    maxAttempts = 2
                    initialDelay = Duration.ofMillis(10)
                    maxDelay = Duration.ofMillis(20)
                    factor = 1.5
                    jitter = 0.0
                }
                timeout = Duration.ofSeconds(1)
                at(Instant.parse("2030-01-01T00:00:00Z"))
                execute {}
            }
        }

        assertEquals(Duration.ofMillis(250), config.pollingInterval)
        assertEquals(Duration.ofMillis(500), config.misfireThreshold)
        assertEquals(Duration.ofMillis(750), config.shutdownTimeout)
        assertEquals(Duration.ofMillis(1_000), config.executionLeaseDuration)
        assertEquals(Duration.ofMillis(250), config.heartbeatInterval)
        val job = config.jobs.single()
        assertEquals("covered", job.description)
        assertEquals(2, job.retryPolicy.maxAttempts)
        assertEquals(Duration.ofSeconds(1), job.timeout)
        assertTrue(job.trigger is OneTimeTrigger)
    }

    @Test
    fun `JobBuilder supports all trigger helpers and rejects duplicate triggers`() {
        assertTrue(JobBuilder("every-kotlin").apply {
            every(1_000.milliseconds)
            execute {}
        }.build().trigger is IntervalTrigger)

        assertTrue(JobBuilder("once").apply {
            once()
            execute {}
        }.build().trigger is OneTimeTrigger)

        assertEquals("Europe/London", (JobBuilder("zone").apply {
            cron("0 9 * * *", ZoneId.of("Europe/London"))
            execute {}
        }.build().trigger as CronTrigger).timeZone)

        listOf<(JobBuilder) -> Unit>(
            { it.every(Duration.ofMinutes(1)) },
            { it.once() },
            { it.at(Instant.now()) },
            { it.cron("0 0 * * *") },
            { it.cron("0 0 * * *", "UTC") }
        ).forEach { secondTrigger ->
            assertThrows(IllegalStateException::class.java) {
                JobBuilder("duplicate").apply {
                    every(Duration.ofMinutes(1))
                    secondTrigger(this)
                    execute {}
                }
            }
        }
    }

    @Test
    fun `JobBuilder rejects missing handler`() {
        assertThrows(IllegalArgumentException::class.java) {
            JobBuilder("missing-handler").apply {
                every(Duration.ofMinutes(1))
            }.build()
        }
    }

    @Test
    fun `RetryPolicy covers validation and delay branches`() {
        assertEquals(Duration.ZERO, RetryPolicy(jitter = 0.0).calculateDelay(0))
        assertEquals(Duration.ofMillis(100), RetryPolicy(
            initialDelay = Duration.ofMillis(100),
            maxDelay = Duration.ofMillis(100),
            factor = 2.0,
            jitter = 0.0
        ).calculateDelay(3))
        assertTrue(RetryPolicy(
            initialDelay = Duration.ZERO,
            maxDelay = Duration.ZERO,
            jitter = 1.0
        ).calculateDelay(1).isZero)

        assertThrows(IllegalArgumentException::class.java) {
            RetryPolicy(initialDelay = Duration.ofMillis(-1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            RetryPolicy(maxDelay = Duration.ofMillis(-1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            RetryPolicy(initialDelay = Duration.ofSeconds(2), maxDelay = Duration.ofSeconds(1))
        }
        assertThrows(IllegalArgumentException::class.java) {
            RetryPolicy(jitter = -0.1)
        }
    }

    @Test
    fun `serializers round trip primitive values`() {
        val duration = Duration.ofMillis(123)
        val instant = Instant.parse("2030-01-01T00:00:00Z")
        val uuid = UUID.randomUUID()

        assertEquals("\"PT0.123S\"", Json.encodeToString(DurationSerializer, duration))
        assertEquals(duration, Json.decodeFromString(DurationSerializer, "\"PT0.123S\""))
        assertEquals("\"2030-01-01T00:00:00Z\"", Json.encodeToString(InstantSerializer, instant))
        assertEquals(instant, Json.decodeFromString(InstantSerializer, "\"2030-01-01T00:00:00Z\""))
        assertEquals("\"$uuid\"", Json.encodeToString(UUIDSerializer, uuid))
        assertEquals(uuid, Json.decodeFromString(UUIDSerializer, "\"$uuid\""))
    }

    @Test
    fun `data classes expose generated copy component and string methods`() {
        val execution = JobExecution(jobId = "generated", scheduledAt = Instant.now(), payload = "payload")
        val copiedExecution = execution.copy(attempt = 2)
        val job = JobDefinition(id = "generated", trigger = IntervalTrigger(Duration.ofMinutes(1)))
        val copiedJob = job.copy(description = "description")

        assertEquals("generated", execution.component2())
        assertEquals(2, copiedExecution.attempt)
        assertEquals("generated", job.component1())
        assertEquals("description", copiedJob.description)
        assertTrue(execution.toString().contains("generated"))
        assertTrue(job.toString().contains("generated"))
        assertEquals(execution, execution.copy())
        assertEquals(job, job.copy())
        assertNotNull(execution.hashCode())
        assertNotNull(job.hashCode())
        assertNull(copiedJob.timeout)
    }
}
