package io.khrona.observability

import io.khrona.core.ExecutionContext
import io.khrona.core.ExecutionOutcome
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.opentelemetry.api.trace.Span
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.UUID

class ConsumerClasspathTest {

    private fun loadConsumerClasspaths(): Properties {
        val stream = checkNotNull(javaClass.classLoader.getResourceAsStream("consumer-classpath.properties")) {
            "Missing consumer-classpath.properties generated test resource"
        }
        return Properties().apply { stream.use { load(it) } }
    }

    private fun runIsolatedConsumer(consumerClasspath: String, mainClass: String): String {
        val javaBin = ProcessHandle.current().info().command().orElse("java")
        val testClassesLocation = File(ConsumerClasspathTest::class.java.protectionDomain.codeSource.location.toURI()).absolutePath
        val fullClasspath = "$consumerClasspath${File.pathSeparator}$testClassesLocation"
        val process = ProcessBuilder(javaBin, "-cp", fullClasspath, mainClass)
            .redirectErrorStream(true)
            .start()
        val output = process.inputStream.bufferedReader().readText()
        val exitCode = process.waitFor()
        assertEquals(0, exitCode, "Isolated consumer $mainClass failed (exit=$exitCode):\n$output")
        return output
    }

    @Test
    fun `documented Micrometer consumer classpath runs without OpenTelemetry dependencies`() {
        val props = loadConsumerClasspaths()
        val output = runIsolatedConsumer(
            props.getProperty("micrometer.classpath"),
            MicrometerConsumerEntrypoint::class.java.name
        )
        assertTrue(output.contains("MICROMETER_CONSUMER_OK"), "Unexpected output:\n$output")
    }

    @Test
    fun `documented OpenTelemetry consumer classpath with opentelemetry-extension-kotlin propagates spans without Micrometer`() {
        val props = loadConsumerClasspaths()
        val output = runIsolatedConsumer(
            props.getProperty("opentelemetry.classpath"),
            OpenTelemetryConsumerEntrypoint::class.java.name
        )
        assertTrue(output.contains("OTEL_CONSUMER_OK"), "Unexpected output:\n$output")
    }
}

object MicrometerConsumerEntrypoint {
    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        try {
            Class.forName("io.opentelemetry.api.OpenTelemetry")
            error("OpenTelemetry API should not be present on Micrometer-only consumer classpath")
        } catch (_: ClassNotFoundException) {
            // Expected
        }

        val registry = SimpleMeterRegistry()
        val observer = MicrometerKhronaObserver(registry)
        val ctx = ExecutionContext(
            executionId = UUID.randomUUID(),
            jobId = "consumer-micrometer-job",
            attempt = 0,
            scheduledAt = Instant.now(),
            payload = null,
            correlationId = "corr-micrometer",
            isStopping = { false }
        )
        observer.onExecutionStarted(ctx, Duration.ofMillis(5))
        val value = observer.aroundExecution(ctx) { "ok" }
        observer.onExecutionCompleted(ctx, ExecutionOutcome.SUCCESS, Duration.ofMillis(10), null)
        check(value == "ok")
        check(
            registry.get("khrona.executions.completed")
                .tag("job_id", "consumer-micrometer-job")
                .tag("outcome", "success")
                .tag("error_type", "none")
                .counter().count() == 1.0
        )
        println("MICROMETER_CONSUMER_OK")
    }
}

object OpenTelemetryConsumerEntrypoint {
    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        try {
            Class.forName("io.micrometer.core.instrument.MeterRegistry")
            error("Micrometer should not be present on OpenTelemetry-only consumer classpath")
        } catch (_: ClassNotFoundException) {
            // Expected
        }

        val spanExporter = InMemorySpanExporter.create()
        val sdk = OpenTelemetrySdk.builder()
            .setTracerProvider(
                SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                    .build()
            )
            .build()

        val observer = OpenTelemetryKhronaObserver(sdk)
        try {
            val ctx = ExecutionContext(
                executionId = UUID.randomUUID(),
                jobId = "consumer-otel-job",
                attempt = 0,
                scheduledAt = Instant.now(),
                payload = null,
                correlationId = "corr-otel",
                isStopping = { false }
            )
            val propagatedSpanId = observer.aroundExecution(ctx) {
                delay(5)
                coroutineScope {
                    async(Dispatchers.Default) {
                        Span.current().spanContext.spanId
                    }.await()
                }
            }
            val exportedSpan = spanExporter.finishedSpanItems.single()
            check(exportedSpan.name == "khrona.job.execute")
            check(exportedSpan.spanContext.spanId == propagatedSpanId)
            println("OTEL_CONSUMER_OK")
        } finally {
            observer.close()
            sdk.close()
        }
    }
}
