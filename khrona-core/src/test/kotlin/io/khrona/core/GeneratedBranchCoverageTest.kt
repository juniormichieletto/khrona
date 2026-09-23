package io.khrona.core

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.polymorphic
import kotlinx.serialization.modules.subclass
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.UUID

class GeneratedBranchCoverageTest {
    private val json = Json {
        ignoreUnknownKeys = true
        serializersModule = SerializersModule {
            polymorphic(Trigger::class) {
                subclass(IntervalTrigger::class)
                subclass(CronTrigger::class)
                subclass(OneTimeTrigger::class)
            }
        }
    }

    @Test
    fun `default argument constructors cover generated branches`() {
        val marker = null
        val id = UUID.randomUUID()
        val instant = Instant.parse("2030-01-01T00:00:00Z")
        val duration = Duration.ofSeconds(1)
        val handler: JobHandler = {}
        val trigger = IntervalTrigger(duration)

        construct<JobExecution>(
            UUID::class.java,
            String::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            ExecutionStatus::class.java,
            Int::class.javaPrimitiveType!!,
            String::class.java,
            String::class.java,
            String::class.java,
            Any::class.java,
            String::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(id, "job", instant, instant, instant, instant, ExecutionStatus.RUNNING, 2, "worker", "lock", "error", "payload", "corr", 0, marker)
        )
        construct<JobExecution>(
            UUID::class.java,
            String::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            ExecutionStatus::class.java,
            Int::class.javaPrimitiveType!!,
            String::class.java,
            String::class.java,
            String::class.java,
            Any::class.java,
            String::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(id, "job", instant, instant, instant, instant, ExecutionStatus.RUNNING, 2, "worker", "lock", "error", "payload", "corr", 8_185, marker)
        )

        construct<JobDefinition>(
            String::class.java,
            String::class.java,
            kotlin.jvm.functions.Function2::class.java,
            Trigger::class.java,
            RetryPolicy::class.java,
            ConcurrencyPolicy::class.java,
            MisfirePolicy::class.java,
            String::class.java,
            Duration::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf("job", "desc", handler, trigger, RetryPolicy.DEFAULT, ConcurrencyPolicy.ALLOW, MisfirePolicy.IGNORE, "lock", duration, 0, marker)
        )
        construct<JobDefinition>(
            String::class.java,
            String::class.java,
            kotlin.jvm.functions.Function2::class.java,
            Trigger::class.java,
            RetryPolicy::class.java,
            ConcurrencyPolicy::class.java,
            MisfirePolicy::class.java,
            String::class.java,
            Duration::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf("job", "desc", handler, trigger, RetryPolicy.DEFAULT, ConcurrencyPolicy.ALLOW, MisfirePolicy.IGNORE, "lock", duration, 502, marker)
        )

        construct<RetryPolicy>(
            Int::class.javaPrimitiveType!!,
            Duration::class.java,
            Duration::class.java,
            Double::class.javaPrimitiveType!!,
            Double::class.javaPrimitiveType!!,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(2, duration, duration.multipliedBy(2), 1.5, 0.0, 0, marker)
        )
        construct<RetryPolicy>(
            Int::class.javaPrimitiveType!!,
            Duration::class.java,
            Duration::class.java,
            Double::class.javaPrimitiveType!!,
            Double::class.javaPrimitiveType!!,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(2, duration, duration.multipliedBy(2), 1.5, 0.0, 31, marker)
        )

        construct<IntervalTrigger>(
            Duration::class.java,
            Duration::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(duration, duration, 0, marker)
        )
        construct<IntervalTrigger>(
            Duration::class.java,
            Duration::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf(duration, duration, 2, marker)
        )

        construct<CronTrigger>(
            String::class.java,
            String::class.java,
            String::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf("0 0 * * *", "ctx", "UTC", 0, marker)
        )
        construct<CronTrigger>(
            String::class.java,
            String::class.java,
            String::class.java,
            Int::class.javaPrimitiveType!!,
            Class.forName("kotlin.jvm.internal.DefaultConstructorMarker"),
            args = arrayOf("0 0 * * *", "ctx", "UTC", 6, marker)
        )
    }

    @Test
    fun `serialization covers generated optional field branches`() {
        val execution = JobExecution(
            id = UUID.randomUUID(),
            jobId = "serialized-execution",
            scheduledAt = Instant.parse("2030-01-01T00:00:00Z"),
            startedAt = Instant.parse("2030-01-01T00:00:01Z"),
            completedAt = Instant.parse("2030-01-01T00:00:02Z"),
            expiresAt = Instant.parse("2030-01-01T00:00:03Z"),
            status = ExecutionStatus.SUCCESS,
            attempt = 2,
            workerId = "worker",
            lockKey = "lock",
            error = "none",
            payload = null,
            correlationId = "corr"
        )
        val encodedExecution = json.encodeToString(execution)
        assertEquals(execution.id, json.decodeFromString<JobExecution>(encodedExecution).id)
        assertEquals("minimal", json.decodeFromString<JobExecution>(
            """{"jobId":"minimal","scheduledAt":"2030-01-01T00:00:00Z"}"""
        ).jobId)

        val retry = RetryPolicy(maxAttempts = 2, initialDelay = Duration.ZERO, maxDelay = Duration.ofSeconds(1), factor = 1.0, jitter = 0.0)
        assertEquals(retry, json.decodeFromString<RetryPolicy>(json.encodeToString(retry)))
        assertEquals(RetryPolicy.DEFAULT, json.decodeFromString<RetryPolicy>("{}"))

        val job = JobDefinition(
            id = "serialized-job",
            description = "desc",
            trigger = OneTimeTrigger(Instant.parse("2030-01-01T00:00:00Z")),
            retryPolicy = retry,
            concurrencyPolicy = ConcurrencyPolicy.ALLOW,
            misfirePolicy = MisfirePolicy.IGNORE,
            lockKey = "lock",
            timeout = Duration.ofSeconds(1)
        )
        val decodedJob = json.decodeFromString<JobDefinition>(json.encodeToString(job))
        assertEquals(job.id, decodedJob.id)
        assertEquals("minimal-job", json.decodeFromString<JobDefinition>(
            """{"id":"minimal-job","trigger":{"type":"once","executionTime":"2030-01-01T00:00:00Z"}}"""
        ).id)

        val interval = IntervalTrigger(Duration.ofSeconds(1), Duration.ofMillis(5))
        assertEquals(Duration.ofMillis(5), json.decodeFromString<IntervalTrigger>(json.encodeToString(interval)).initialDelay)
        assertEquals(Duration.ZERO, json.decodeFromString<IntervalTrigger>("""{"interval":"PT1S"}""").initialDelay)

        val once = OneTimeTrigger(Instant.parse("2030-01-01T00:00:00Z"))
        assertEquals(once.executionTime, json.decodeFromString<OneTimeTrigger>(json.encodeToString(once)).executionTime)

        val cron = CronTrigger("0 0 * * *", "ctx", "Europe/London")
        assertEquals("Europe/London", json.decodeFromString<CronTrigger>(json.encodeToString(cron)).timeZone)
        assertEquals("UTC", json.decodeFromString<CronTrigger>("""{"expression":"0 0 * * *"}""").timeZone)
    }

    @Test
    fun `copy default helpers cover generated data class branches`() {
        val instant = Instant.parse("2030-01-01T00:00:00Z")
        val execution = JobExecution(
            id = UUID.randomUUID(),
            jobId = "copy-execution",
            scheduledAt = instant,
            startedAt = instant,
            completedAt = instant,
            expiresAt = instant,
            status = ExecutionStatus.RUNNING,
            attempt = 1,
            workerId = "worker",
            lockKey = "lock",
            error = "error",
            payload = "payload",
            correlationId = "corr"
        )
        invokeStatic(
            JobExecution::class.java,
            "copy\$default",
            arrayOf(
                JobExecution::class.java,
                UUID::class.java,
                String::class.java,
                Instant::class.java,
                Instant::class.java,
                Instant::class.java,
                Instant::class.java,
                ExecutionStatus::class.java,
                Int::class.javaPrimitiveType!!,
                String::class.java,
                String::class.java,
                String::class.java,
                Any::class.java,
                String::class.java,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(execution, UUID.randomUUID(), "job", instant, instant, instant, instant, ExecutionStatus.SUCCESS, 2, "worker-2", "lock-2", "error-2", "payload-2", "corr-2", 0, null)
        )
        invokeStatic(
            JobExecution::class.java,
            "copy\$default",
            arrayOf(
                JobExecution::class.java,
                UUID::class.java,
                String::class.java,
                Instant::class.java,
                Instant::class.java,
                Instant::class.java,
                Instant::class.java,
                ExecutionStatus::class.java,
                Int::class.javaPrimitiveType!!,
                String::class.java,
                String::class.java,
                String::class.java,
                Any::class.java,
                String::class.java,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(execution, null, null, null, null, null, null, null, 0, null, null, null, null, null, 8_191, null)
        )

        val retry = RetryPolicy(maxAttempts = 2, initialDelay = Duration.ZERO, maxDelay = Duration.ofSeconds(1), factor = 1.0, jitter = 0.0)
        invokeStatic(
            RetryPolicy::class.java,
            "copy\$default",
            arrayOf(
                RetryPolicy::class.java,
                Int::class.javaPrimitiveType!!,
                Duration::class.java,
                Duration::class.java,
                Double::class.javaPrimitiveType!!,
                Double::class.javaPrimitiveType!!,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(retry, 3, Duration.ofMillis(1), Duration.ofMillis(2), 2.0, 0.1, 0, null)
        )
        invokeStatic(
            RetryPolicy::class.java,
            "copy\$default",
            arrayOf(
                RetryPolicy::class.java,
                Int::class.javaPrimitiveType!!,
                Duration::class.java,
                Duration::class.java,
                Double::class.javaPrimitiveType!!,
                Double::class.javaPrimitiveType!!,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(retry, 0, null, null, 0.0, 0.0, 31, null)
        )

        val job = JobDefinition(id = "copy-job", trigger = IntervalTrigger(Duration.ofSeconds(1)))
        invokeStatic(
            JobDefinition::class.java,
            "copy\$default",
            arrayOf(
                JobDefinition::class.java,
                String::class.java,
                String::class.java,
                kotlin.jvm.functions.Function2::class.java,
                Trigger::class.java,
                RetryPolicy::class.java,
                ConcurrencyPolicy::class.java,
                MisfirePolicy::class.java,
                String::class.java,
                Duration::class.java,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(job, "job-2", "desc", job.handler, job.trigger, retry, ConcurrencyPolicy.ALLOW, MisfirePolicy.IGNORE, "lock", Duration.ofSeconds(1), 0, null)
        )
        invokeStatic(
            JobDefinition::class.java,
            "copy\$default",
            arrayOf(
                JobDefinition::class.java,
                String::class.java,
                String::class.java,
                kotlin.jvm.functions.Function2::class.java,
                Trigger::class.java,
                RetryPolicy::class.java,
                ConcurrencyPolicy::class.java,
                MisfirePolicy::class.java,
                String::class.java,
                Duration::class.java,
                Int::class.javaPrimitiveType!!,
                Any::class.java
            ),
            arrayOf(job, null, null, null, null, null, null, null, null, null, 511, null)
        )
    }

    @Test
    fun `serialization constructors and cron errors cover generated branches`() {
        val marker = null
        val serializationMarker = Class.forName("kotlinx.serialization.internal.SerializationConstructorMarker")
        val instant = Instant.parse("2030-01-01T00:00:00Z")
        val duration = Duration.ofSeconds(1)

        construct<JobExecution>(
            Int::class.javaPrimitiveType!!,
            UUID::class.java,
            String::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            Instant::class.java,
            ExecutionStatus::class.java,
            Int::class.javaPrimitiveType!!,
            String::class.java,
            String::class.java,
            String::class.java,
            Any::class.java,
            String::class.java,
            serializationMarker,
            args = arrayOf(8_191, UUID.randomUUID(), "job", instant, instant, instant, instant, ExecutionStatus.RUNNING, 1, "worker", "lock", "error", null, "corr", marker)
        )
        construct<JobDefinition>(
            Int::class.javaPrimitiveType!!,
            String::class.java,
            String::class.java,
            Trigger::class.java,
            RetryPolicy::class.java,
            ConcurrencyPolicy::class.java,
            MisfirePolicy::class.java,
            String::class.java,
            Duration::class.java,
            serializationMarker,
            args = arrayOf(511, "job", "desc", IntervalTrigger(duration), RetryPolicy.DEFAULT, ConcurrencyPolicy.ALLOW, MisfirePolicy.IGNORE, "lock", duration, marker)
        )
        construct<RetryPolicy>(
            Int::class.javaPrimitiveType!!,
            Int::class.javaPrimitiveType!!,
            Duration::class.java,
            Duration::class.java,
            Double::class.javaPrimitiveType!!,
            Double::class.javaPrimitiveType!!,
            serializationMarker,
            args = arrayOf(31, 2, duration, duration.multipliedBy(2), 1.5, 0.0, marker)
        )
        construct<IntervalTrigger>(
            Int::class.javaPrimitiveType!!,
            Duration::class.java,
            Duration::class.java,
            serializationMarker,
            args = arrayOf(3, duration, duration, marker)
        )
        construct<CronTrigger>(
            Int::class.javaPrimitiveType!!,
            String::class.java,
            String::class.java,
            String::class.java,
            serializationMarker,
            args = arrayOf(7, "0 0 * * *", "ctx", "UTC", marker)
        )

        assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("*", "job-context")
        }
        assertThrows(IllegalArgumentException::class.java) {
            CronTrigger("0 0 * * *", "job-context", "Invalid/Zone")
        }
    }

    @Test
    fun `companion serializers are initialized`() {
        assertNotNull(JobExecution.serializer())
        assertNotNull(JobDefinition.serializer())
        assertNotNull(RetryPolicy.serializer())
        assertNotNull(ExecutionStatus.entries)
        assertNotNull(MisfirePolicy.entries)
    }

    private inline fun <reified T> construct(vararg parameterTypes: Class<*>, args: Array<Any?>): T {
        val constructor = T::class.java.getDeclaredConstructor(*parameterTypes)
        constructor.isAccessible = true
        return constructor.newInstance(*args) as T
    }

    private fun invokeStatic(target: Class<*>, name: String, parameterTypes: Array<Class<*>>, args: Array<Any?>): Any? {
        val method = target.getDeclaredMethod(name, *parameterTypes)
        method.isAccessible = true
        return method.invoke(null, *args)
    }
}
