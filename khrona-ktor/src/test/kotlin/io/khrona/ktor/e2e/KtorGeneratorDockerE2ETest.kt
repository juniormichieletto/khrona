package io.khrona.ktor.e2e

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.utility.MountableFile
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration

/**
 * End-to-end Dockerized test suite that scaffolds a brand-new Ktor application from scratch inside a
 * Docker container using the official Ktor Project Generator (`ktor new`), wires Khrona (`khrona-ktor`
 * + storage backend) from `mavenLocal()`, starts the Ktor server inside the container, and validates
 * Khrona's core use cases and edge cases over HTTP.
 *
 * Excluded from default `./gradlew test` runs via `@Tag("e2e")`.
 * Run explicitly with:
 * ```bash
 * ./gradlew :khrona-ktor:e2eTest
 * ```
 */
@Tag("e2e")
class KtorGeneratorDockerE2ETest {

    companion object {
        private val httpClient: HttpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()

        private val khronaVersion: String = System.getProperty("khrona.version", "0.6.0")

        /**
         * Base image with JDK 21, curl, unzip, and the official Ktor CLI (`ktor`) pre-installed so each
         * test container can run `ktor new` inside the container.
         */
        private val ktorGeneratorImage: ImageFromDockerfile = ImageFromDockerfile("khrona-ktor-e2e-runner", false)
            .withDockerfileFromBuilder { builder ->
                builder
                    .from("eclipse-temurin:21-jdk")
                    .run(
                        "apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates && " +
                            "ARCH=\$(dpkg --print-architecture) && " +
                            "if [ \"\$ARCH\" = \"arm64\" ]; then KTOR_BIN=ktor_linux_arm64; else KTOR_BIN=ktor_linux_amd64; fi && " +
                            "curl -fsSL https://github.com/ktorio/ktor-cli/releases/download/0.5.0/\$KTOR_BIN -o /usr/local/bin/ktor && " +
                            "chmod +x /usr/local/bin/ktor && " +
                            "rm -rf /var/lib/apt/lists/*"
                    )
                    .workDir("/workspace")
                    .build()
            }
    }

    @Test
    fun `scaffolds Ktor project inside Docker with MemoryJobStore and validates triggers retries fencing and correlationId`() {
        val script = buildBootstrapScript("khrona-memory-app")
        val extraDeps = """
            dependencies {
                implementation("io.github.juniormichieletto:khrona-ktor:$khronaVersion")
                implementation("io.github.juniormichieletto:khrona-store-memory:$khronaVersion")
                implementation("io.github.juniormichieletto:khrona-observability:$khronaVersion")
                implementation("io.micrometer:micrometer-core:1.14.5")
            }
        """.trimIndent()

        createKtorRunnerContainer(script, extraDeps, memoryUseCaseRoutingSource()).use { appContainer ->
            appContainer.start()
            val baseUrl = "http://${appContainer.host}:${appContainer.getMappedPort(8080)}"

            // Trigger manual job with custom correlationId and structured payload
            val triggerResponse = httpPost(
                url = "$baseUrl/trigger",
                headers = mapOf("X-Correlation-Id" to "e2e-corr-memory-1")
            )
            assertEquals(202, triggerResponse.statusCode())

            // Poll /status until interval, once, cron registration, manual trigger, retry-to-success, and management checks all complete
            val status = awaitStatus(baseUrl) { json ->
                json["onceRuns"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["intervalRuns"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                    json["manualRuns"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["retrySuccesses"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["deadLetterAttempts"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                    json["fencingPassed"]?.jsonPrimitive?.boolean == true &&
                    json["cronRegistered"]?.jsonPrimitive?.boolean == true &&
                    json["managementVerified"]?.jsonPrimitive?.boolean == true &&
                    json["checkpointShutdownVerified"]?.jsonPrimitive?.boolean == true
            }

            assertEquals("e2e-corr-memory-1", status["lastManualCorrelationId"]?.jsonPrimitive?.content)
            assertEquals("hello-from-e2e:42:true", status["lastManualPayload"]?.jsonPrimitive?.content)
            assertEquals(true, status["fencingPassed"]?.jsonPrimitive?.boolean)
            assertEquals(true, status["managementVerified"]?.jsonPrimitive?.boolean)
            assertEquals(true, status["checkpointShutdownVerified"]?.jsonPrimitive?.boolean)

            // Verify Micrometer metrics and bounded tag cardinality via /metrics-summary
            val metrics = awaitJson("$baseUrl/metrics-summary") { json ->
                (json["deadLetterSize"]?.jsonPrimitive?.int ?: 0) >= 1 &&
                    (json["completedDeadLettered"]?.jsonPrimitive?.int ?: 0) >= 1 &&
                    (json["completedSuccess"]?.jsonPrimitive?.int ?: 0) >= 1 &&
                    (json["completedFailedRetry"]?.jsonPrimitive?.int ?: 0) >= 1
            }
            assertTrue((metrics["started"]?.jsonPrimitive?.int ?: 0) >= 1)
            assertTrue((metrics["storeOps"]?.jsonPrimitive?.int ?: 0) >= 1)
            assertEquals(false, metrics["unboundedTagLeak"]?.jsonPrimitive?.boolean)

            // Verify host-owned /health lifecycle: UP -> outage (DOWN 503) -> recovery (UP 200)
            val initialHealth = httpGet("$baseUrl/health")
            assertEquals(200, initialHealth.statusCode())
            val initialHealthJson = Json.parseToJsonElement(initialHealth.body()) as JsonObject
            assertEquals("UP", initialHealthJson["status"]?.jsonPrimitive?.content)
            assertEquals("UP", initialHealthJson["storeStatus"]?.jsonPrimitive?.content)
            assertEquals(true, initialHealthJson["isHealthy"]?.jsonPrimitive?.boolean)
            assertEquals(true, initialHealthJson["isReady"]?.jsonPrimitive?.boolean)

            assertEquals(200, httpPost("$baseUrl/admin/outage/on").statusCode())
            val outageHealth = awaitHttpJson("$baseUrl/health", expectedStatus = 503) { json ->
                json["status"]?.jsonPrimitive?.content == "DOWN" &&
                    json["storeStatus"]?.jsonPrimitive?.content == "DOWN"
            }
            assertEquals(false, outageHealth["isHealthy"]?.jsonPrimitive?.boolean)
            assertEquals(false, outageHealth["isReady"]?.jsonPrimitive?.boolean)

            assertEquals(200, httpPost("$baseUrl/admin/outage/off").statusCode())
            val recoveredHealth = awaitHttpJson("$baseUrl/health", expectedStatus = 200) { json ->
                json["status"]?.jsonPrimitive?.content == "UP" &&
                    json["storeStatus"]?.jsonPrimitive?.content == "UP"
            }
            assertEquals(true, recoveredHealth["isHealthy"]?.jsonPrimitive?.boolean)
            assertEquals(true, recoveredHealth["isReady"]?.jsonPrimitive?.boolean)
        }
    }

    @Test
    fun `scaffolds Ktor project inside Docker with JdbcJobStore PostgreSQL and OpenTelemetry and validates persistence FORBID REPLACE misfires retention tracing metrics and health`() {
        Network.newNetwork().use { network ->
            PostgreSQLContainer("postgres:16-alpine")
                .withNetwork(network)
                .withNetworkAliases("postgres-db")
                .withDatabaseName("khrona_e2e")
                .withUsername("khrona")
                .withPassword("khrona")
                .use { postgres ->
                    postgres.start()

                    val script = buildBootstrapScript("khrona-jdbc-app")
                    val extraDeps = """
                        dependencies {
                            implementation("io.github.juniormichieletto:khrona-ktor:$khronaVersion")
                            implementation("io.github.juniormichieletto:khrona-store-jdbc:$khronaVersion")
                            implementation("io.github.juniormichieletto:khrona-observability:$khronaVersion")
                            implementation("io.opentelemetry:opentelemetry-api:1.47.0")
                            implementation("io.opentelemetry:opentelemetry-extension-kotlin:1.47.0")
                            implementation("io.opentelemetry:opentelemetry-sdk-testing:1.47.0")
                            implementation("com.zaxxer:HikariCP:6.2.1")
                            runtimeOnly("org.postgresql:postgresql:42.7.5")
                        }
                    """.trimIndent()

                    createKtorRunnerContainer(script, extraDeps, jdbcPostgresUseCaseRoutingSource())
                        .withNetwork(network)
                        .withEnv("DB_URL", "jdbc:postgresql://postgres-db:5432/khrona_e2e")
                        .withEnv("DB_USER", "khrona")
                        .withEnv("DB_PASSWORD", "khrona")
                        .use { appContainer ->
                            appContainer.start()
                            val baseUrl = "http://${appContainer.host}:${appContainer.getMappedPort(8080)}"

                            val status = awaitStatus(baseUrl) { json ->
                                json["intervalRuns"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                                    json["replaceCancelledFirst"]?.jsonPrimitive?.boolean == true &&
                                    json["replaceCompletedSecond"]?.jsonPrimitive?.boolean == true &&
                                    json["forbidVerified"]?.jsonPrimitive?.boolean == true &&
                                    json["misfireIgnored"]?.jsonPrimitive?.boolean == true &&
                                    json["retentionCleaned"]?.jsonPrimitive?.boolean == true &&
                                    json["fencingVerified"]?.jsonPrimitive?.boolean == true &&
                                    json["managementVerified"]?.jsonPrimitive?.boolean == true
                            }

                            assertTrue(status["replaceCancelledFirst"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["replaceCompletedSecond"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["forbidVerified"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["misfireIgnored"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["retentionCleaned"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["fencingVerified"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["managementVerified"]?.jsonPrimitive?.boolean == true)

                            // Verify OpenTelemetryKhronaObserver spans, coroutine context propagation, and bounded metrics
                            val otel = awaitJson("$baseUrl/otel-summary") { json ->
                                (json["otelSpanCount"]?.jsonPrimitive?.int ?: 0) >= 2 &&
                                    json["otelContextPropagated"]?.jsonPrimitive?.boolean == true &&
                                    (json["otelCompletedCount"]?.jsonPrimitive?.long ?: 0L) >= 2L &&
                                    (json["otelStoreOpCount"]?.jsonPrimitive?.long ?: 0L) >= 1L
                            }
                            assertTrue((otel["otelSpanCount"]?.jsonPrimitive?.int ?: 0) >= 2)
                            assertEquals(true, otel["otelContextPropagated"]?.jsonPrimitive?.boolean)
                            assertTrue((otel["otelCompletedCount"]?.jsonPrimitive?.long ?: 0L) >= 2L)
                            assertTrue((otel["otelStoreOpCount"]?.jsonPrimitive?.long ?: 0L) >= 1L)
                            assertEquals(false, otel["unboundedMetricAttributeLeak"]?.jsonPrimitive?.boolean)

                            // Verify host-owned /health endpoint backed by scheduler.health() against live PostgreSQL
                            val health = awaitHttpJson("$baseUrl/health", expectedStatus = 200) { json ->
                                json["status"]?.jsonPrimitive?.content == "UP" &&
                                    json["storeStatus"]?.jsonPrimitive?.content == "UP"
                            }
                            assertEquals(true, health["isHealthy"]?.jsonPrimitive?.boolean)
                            assertEquals(true, health["isReady"]?.jsonPrimitive?.boolean)
                        }
                }
        }
    }

    @Test
    fun `scaffolds Ktor project inside Docker with RedisJobStore and validates atomic claiming heartbeats REPLACE and stale recovery`() {
        Network.newNetwork().use { network ->
            GenericContainer("redis:7-alpine")
                .withNetwork(network)
                .withNetworkAliases("redis-server")
                .withExposedPorts(6379)
                .use { redis ->
                    redis.start()

                    val script = buildBootstrapScript("khrona-redis-app")
                    val extraDeps = """
                        dependencies {
                            implementation("io.github.juniormichieletto:khrona-ktor:$khronaVersion")
                            implementation("io.github.juniormichieletto:khrona-store-redis:$khronaVersion")
                        }
                    """.trimIndent()

                    createKtorRunnerContainer(script, extraDeps, redisUseCaseRoutingSource())
                        .withNetwork(network)
                        .withEnv("REDIS_URI", "redis://redis-server:6379")
                        .use { appContainer ->
                            appContainer.start()
                            val baseUrl = "http://${appContainer.host}:${appContainer.getMappedPort(8080)}"

                            val status = awaitStatus(baseUrl) { json ->
                                json["intervalRuns"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                                    json["replaceCancelledFirst"]?.jsonPrimitive?.boolean == true &&
                                    json["replaceCompletedSecond"]?.jsonPrimitive?.boolean == true &&
                                    json["heartbeatExtended"]?.jsonPrimitive?.boolean == true &&
                                    json["staleRecovered"]?.jsonPrimitive?.boolean == true &&
                                    json["replaceSuperseded"]?.jsonPrimitive?.boolean == true &&
                                    json["retentionCleaned"]?.jsonPrimitive?.boolean == true &&
                                    json["fencingVerified"]?.jsonPrimitive?.boolean == true
                            }

                            assertTrue(status["replaceCancelledFirst"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["replaceCompletedSecond"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["heartbeatExtended"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["staleRecovered"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["replaceSuperseded"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["retentionCleaned"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["fencingVerified"]?.jsonPrimitive?.boolean == true)

                            // Verify host-owned /health endpoint backed by scheduler.health() against live Redis
                            val health = awaitHttpJson("$baseUrl/health", expectedStatus = 200) { json ->
                                json["status"]?.jsonPrimitive?.content == "UP" &&
                                    json["storeStatus"]?.jsonPrimitive?.content == "UP"
                            }
                            assertEquals(true, health["isHealthy"]?.jsonPrimitive?.boolean)
                            assertEquals(true, health["isReady"]?.jsonPrimitive?.boolean)
                        }
                }
        }
    }

    private fun createKtorRunnerContainer(
        bootstrapScript: String,
        extraDependenciesBlock: String,
        routingSource: String
    ): GenericContainer<*> {
        val m2Repo: Path = Paths.get(System.getProperty("user.home"), ".m2", "repository")
        val gradleUserHome: Path = System.getenv("GRADLE_USER_HOME")?.takeIf { it.isNotBlank() }?.let { Paths.get(it) }
            ?: Paths.get(System.getProperty("user.home"), ".gradle")
        val containerGradleHome: Path = gradleUserHome.resolve("khrona-e2e-container-home")
        Files.createDirectories(m2Repo)
        Files.createDirectories(containerGradleHome)

        val tempScript = Files.createTempFile("khrona-e2e-bootstrap-", ".sh")
        Files.writeString(tempScript, bootstrapScript)
        tempScript.toFile().setExecutable(true)
        tempScript.toFile().deleteOnExit()

        val tempDeps = Files.createTempFile("khrona-e2e-deps-", ".gradle.kts")
        Files.writeString(tempDeps, "\n" + extraDependenciesBlock + "\n")
        tempDeps.toFile().deleteOnExit()

        val tempRouting = Files.createTempFile("khrona-e2e-routing-", ".kt")
        Files.writeString(tempRouting, routingSource)
        tempRouting.toFile().deleteOnExit()

        return GenericContainer(ktorGeneratorImage)
            .withExposedPorts(8080)
            .withFileSystemBind(m2Repo.toAbsolutePath().toString(), "/root/.m2/repository", BindMode.READ_ONLY)
            .withFileSystemBind(containerGradleHome.toAbsolutePath().toString(), "/root/.gradle", BindMode.READ_WRITE)
            .apply {
                if (Files.isDirectory(gradleUserHome.resolve("caches"))) {
                    withFileSystemBind(
                        gradleUserHome.resolve("caches").toAbsolutePath().toString(),
                        "/gradle-ro-cache",
                        BindMode.READ_ONLY
                    )
                    withEnv("GRADLE_RO_DEP_CACHE", "/gradle-ro-cache")
                }
                if (Files.isDirectory(gradleUserHome.resolve("wrapper"))) {
                    withFileSystemBind(
                        gradleUserHome.resolve("wrapper").toAbsolutePath().toString(),
                        "/root/.gradle/wrapper",
                        BindMode.READ_WRITE
                    )
                }
            }
            .withCopyFileToContainer(MountableFile.forHostPath(tempScript, 493), "/workspace/bootstrap.sh")
            .withCopyFileToContainer(MountableFile.forHostPath(tempDeps), "/workspace/extra-deps.gradle.kts")
            .withCopyFileToContainer(MountableFile.forHostPath(tempRouting), "/workspace/Routing.kt.template")
            .withCommand("/bin/bash", "/workspace/bootstrap.sh")
            .withLogConsumer { frame -> print(frame.utf8String) }
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(8080)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3))
            )
    }

    private fun buildBootstrapScript(projectName: String): String = """
        #!/usr/bin/env bash
        set -euo pipefail

        echo "=== 1. Generating Ktor project from scratch via official Ktor CLI ==="
        rm -rf /workspace/$projectName
        ktor new /workspace/$projectName
        cd /workspace/$projectName

        echo "=== 2. Adding mavenLocal() to generated settings.gradle.kts ==="
        sed -i 's/mavenCentral()/mavenLocal()\n        mavenCentral()/g' settings.gradle.kts

        echo "=== 3. Appending Khrona dependencies into generated build.gradle.kts ==="
        cat /workspace/extra-deps.gradle.kts >> build.gradle.kts

        echo "=== 4. Writing Ktor Routing + Khrona Scheduler configuration ==="
        cp /workspace/Routing.kt.template src/main/kotlin/Routing.kt

        echo "=== 5. Starting Ktor application ==="
        exec ./gradlew run --no-daemon --console=plain
    """.trimIndent()

    private fun memoryUseCaseRoutingSource(): String = """
        package com.example.com

        import io.khrona.core.*
        import io.khrona.ktor.*
        import io.khrona.observability.MicrometerKhronaObserver
        import io.khrona.store.memory.MemoryJobStore
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.request.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import io.micrometer.core.instrument.simple.SimpleMeterRegistry
        import kotlinx.coroutines.CompletableDeferred
        import kotlinx.coroutines.async
        import kotlinx.coroutines.awaitCancellation
        import kotlinx.coroutines.delay
        import kotlinx.coroutines.launch
        import kotlinx.coroutines.runBlocking
        import kotlinx.coroutines.withTimeout
        import org.slf4j.MDC
        import java.time.Duration
        import java.time.Instant
        import java.time.ZoneId
        import java.util.UUID
        import java.util.concurrent.atomic.AtomicBoolean
        import java.util.concurrent.atomic.AtomicInteger
        import java.util.concurrent.atomic.AtomicReference
        import kotlin.time.Duration.Companion.milliseconds
        import kotlin.time.Duration.Companion.seconds

        fun Application.configureRouting() {
            val baseStore = MemoryJobStore()
            val outageActive = AtomicBoolean(false)
            val meterRegistry = SimpleMeterRegistry()
            val micrometerObserver = MicrometerKhronaObserver(meterRegistry)
            val store = object : JobStore by baseStore {
                private fun ensureAvailable() {
                    if (outageActive.get()) {
                        throw IllegalStateException("Simulated store outage")
                    }
                }

                override suspend fun checkHealth(): StoreHealth {
                    ensureAvailable()
                    return baseStore.checkHealth()
                }

                override suspend fun listEligibleExecutions(now: Instant, limit: Int): List<JobExecution> {
                    ensureAvailable()
                    return baseStore.listEligibleExecutions(now, limit)
                }

                override suspend fun listEligibleExecutions(
                    now: Instant,
                    limit: Int,
                    jobIds: Set<String>?
                ): List<JobExecution> {
                    ensureAvailable()
                    return baseStore.listEligibleExecutions(now, limit, jobIds)
                }

                override suspend fun countExecutions(query: JobExecutionQuery): Long {
                    ensureAvailable()
                    return baseStore.countExecutions(query)
                }

                override suspend fun resetExpiredExecutions(now: Instant): Int {
                    ensureAvailable()
                    return baseStore.resetExpiredExecutions(now)
                }
            }.observedBy(micrometerObserver)

            val onceRuns = AtomicInteger(0)
            val intervalRuns = AtomicInteger(0)
            val manualRuns = AtomicInteger(0)
            val retryAttempts = AtomicInteger(0)
            val retrySuccesses = AtomicInteger(0)
            val deadLetterAttempts = AtomicInteger(0)
            val lastManualCorrelationId = AtomicReference("none")
            val lastManualPayload = AtomicReference("none")
            val fencingPassed = AtomicBoolean(false)
            val contextVerified = AtomicBoolean(false)
            val managementVerified = AtomicBoolean(false)
            val checkpointShutdownVerified = AtomicBoolean(false)
            val stopTargetStarted = CompletableDeferred<UUID>()
            val pausedJobRuns = AtomicInteger(0)

            installKhrona {
                this.store = store
                this.observer = micrometerObserver
                healthPollFailureThreshold = 1
                deadLetterSamplingInterval(200.milliseconds)
                pollingInterval(100.milliseconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
                retention {
                    enabled = true
                    cleanupJitter = 0.1
                }
            }

            runBlocking {
                scheduler {
                    pollingInterval(100.milliseconds)

                    // UC-3: Startup one-time job
                    job("startup-once") {
                        once()
                        execute {
                            onceRuns.incrementAndGet()
                        }
                    }

                    // UC-1: Fixed-cadence interval job
                    job("interval-sync") {
                        every(200.milliseconds)
                        execute {
                            intervalRuns.incrementAndGet()
                        }
                    }

                    // UC-2: Timezone-aware cron job
                    job("daily-cron") {
                        cron("0 9 * * *", ZoneId.of("UTC"))
                        execute {}
                    }

                    // UC-4: On-demand manual trigger with structured payload, ExecutionContext, & MDC correlationId
                    job("manual-webhook") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        concurrencyPolicy = ConcurrencyPolicy.ALLOW
                        execute { payload ->
                            checkpoint()
                            @Suppress("UNCHECKED_CAST")
                            val map = payload as? Map<String, Any?>
                            lastManualPayload.set("${'$'}{map?.get("msg")}:${'$'}{map?.get("count")}:${'$'}{map?.get("ok")}")
                            lastManualCorrelationId.set(MDC.get("correlationId") ?: "missing")
                            if (executionId != UUID(0L, 0L) && attempt == 0 && scheduledAt <= Instant.now().plusSeconds(5)) {
                                contextVerified.set(true)
                            }
                            manualRuns.incrementAndGet()
                        }
                    }

                    // Management pause/resume & local stop E2E jobs
                    job("managed-pause-job") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        paused = true
                        execute {
                            pausedJobRuns.incrementAndGet()
                        }
                    }

                    job("managed-stop-job") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        execute {
                            stopTargetStarted.complete(executionId)
                            awaitCancellation()
                        }
                    }

                    // UC-9: Transient fault recovery (fails once, succeeds on retry)
                    job("flaky-retry-job") {
                        once()
                        retry {
                            maxAttempts = 3
                            initialDelay(100.milliseconds)
                            maxDelay(500.milliseconds)
                        }
                        execute {
                            val attempt = retryAttempts.incrementAndGet()
                            if (attempt < 2) {
                                throw IllegalStateException("Simulated transient error on attempt ${'$'}attempt")
                            }
                            retrySuccesses.incrementAndGet()
                        }
                    }

                    // UC-9 & EC-C1: Timeout leading to DEAD_LETTERED isolation
                    job("timeout-dlq-job") {
                        once()
                        timeout(100.milliseconds)
                        retry {
                            maxAttempts = 2
                            initialDelay(100.milliseconds)
                        }
                        execute {
                            deadLetterAttempts.incrementAndGet()
                            kotlinx.coroutines.delay(1_000)
                        }
                    }
                }

                // EC-A3 & EC-A5: Verify heartbeat, status transition fencing, and saveExecution startup race protection directly against the store
                val fencedExec = JobExecution(jobId = "fenced-direct", scheduledAt = Instant.now())
                store.saveExecution(fencedExec)
                val claimed1 = store.claimExecution(fencedExec.id, "worker-1", Duration.ofSeconds(-5))
                val expiredHbRejected = !store.heartbeat(fencedExec.id, "worker-1", Duration.ofMinutes(1))
                val claimed2 = store.claimExecution(fencedExec.id, "worker-2", Duration.ofMinutes(1))
                store.saveExecution(fencedExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileClaimed = store.getExecution(fencedExec.id)?.status == ExecutionStatus.CLAIMED &&
                    store.getExecution(fencedExec.id)?.workerId == "worker-2"
                val staleHbRejected = !store.heartbeat(fencedExec.id, "worker-1", Duration.ofMinutes(1))
                val staleStatusRejected = !store.updateExecutionStatus(fencedExec.id, ExecutionStatus.SUCCESS, workerId = "worker-1")
                val ownerStatusAccepted = store.updateExecutionStatus(fencedExec.id, ExecutionStatus.SUCCESS, workerId = "worker-2")
                store.saveExecution(fencedExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileTerminal = store.getExecution(fencedExec.id)?.status == ExecutionStatus.SUCCESS
                fencingPassed.set(
                    claimed1 && expiredHbRejected && claimed2 && latePendingIgnoredWhileClaimed &&
                        staleHbRejected && staleStatusRejected && ownerStatusAccepted && latePendingIgnoredWhileTerminal
                )

                // Verify shutdown checkpoint releases back to PENDING on stop()
                val cpStore = MemoryJobStore()
                val cpEntered = CompletableDeferred<Unit>()
                val cpProceed = CompletableDeferred<Unit>()
                val cpAfter = AtomicBoolean(false)
                val cpScheduler = Scheduler(
                    KhronaConfig().apply {
                        this.store = cpStore
                        pollingInterval = Duration.ofMillis(50)
                        shutdownTimeout = Duration.ofSeconds(5)
                        job("cp-shutdown-job") {
                            once()
                            execute {
                                cpEntered.complete(Unit)
                                cpProceed.await()
                                checkpoint()
                                cpAfter.set(true)
                            }
                        }
                    }
                )
                cpScheduler.start()
                withTimeout(5_000) { cpEntered.await() }
                val cpStop = async { cpScheduler.stop() }
                delay(50)
                cpProceed.complete(Unit)
                withTimeout(5_000) { cpStop.await() }
                val cpExec = cpStore.listExecutions(JobExecutionQuery(jobId = "cp-shutdown-job")).singleOrNull()
                checkpointShutdownVerified.set(!cpAfter.get() && cpExec?.status == ExecutionStatus.PENDING && cpExec.workerId == null)

                val khronaScheduler = this@configureRouting.plugin(KhronaPlugin).scheduler
                this@configureRouting.monitor.subscribe(ApplicationStarted) { app ->
                    app.launch {
                        // Start paused job while paused, confirm overview & PENDING status, then resume
                        val queuedPaused = khronaScheduler.startJob("managed-pause-job")
                        val pausedOverview = khronaScheduler.getJobOverview("managed-pause-job")
                        delay(250)
                        val stayedPaused = pausedJobRuns.get() == 0 && pausedOverview?.paused == true
                        khronaScheduler.resumeJob("managed-pause-job")
                        withTimeout(5_000) {
                            while (pausedJobRuns.get() < 1) {
                                delay(25)
                            }
                        }

                        // Start stoppable job, stop locally, and verify CANCELLED + history + overviews
                        val startedStop = khronaScheduler.startJob("managed-stop-job")
                        val startedStopId = withTimeout(5_000) { stopTargetStarted.await() }
                        val stopRes = khronaScheduler.stopJob("managed-stop-job")
                        val stoppedExec = store.getExecution(startedStopId)
                        val history = khronaScheduler.listJobHistory("managed-stop-job", limit = 10)
                        val overviews = khronaScheduler.listJobOverviews()
                        managementVerified.set(
                            queuedPaused.success && stayedPaused && pausedJobRuns.get() == 1 &&
                                startedStop.success && stopRes.success &&
                                stopRes.affectedExecutionIds == listOf(startedStopId) &&
                                stoppedExec?.status == ExecutionStatus.CANCELLED &&
                                history.any { it.id == startedStopId && it.status == ExecutionStatus.CANCELLED } &&
                                overviews.any { it.job.id == "managed-stop-job" && it.hasLocalHandler }
                        )
                    }
                }
            }

            routing {
                get("/health") {
                    val h = this@configureRouting.plugin(KhronaPlugin).scheduler.health()
                    val code = if (h.isReady) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable
                    val body = ""${'"'}{
                        "status": "${'$'}{h.status.name}",
                        "storeStatus": "${'$'}{h.storeHealth.status.name}",
                        "isHealthy": ${'$'}{h.isHealthy},
                        "isReady": ${'$'}{h.isReady},
                        "consecutivePollFailures": ${'$'}{h.consecutivePollFailures}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json, code)
                }

                post("/admin/outage/on") {
                    outageActive.set(true)
                    call.respondText("Outage ON", ContentType.Text.Plain, HttpStatusCode.OK)
                }

                post("/admin/outage/off") {
                    outageActive.set(false)
                    call.respondText("Outage OFF", ContentType.Text.Plain, HttpStatusCode.OK)
                }

                get("/metrics-summary") {
                    val started = meterRegistry.find("khrona.executions.started").counters().sumOf { it.count() }.toInt()
                    val completedSuccess = meterRegistry.find("khrona.executions.completed").tag("outcome", "success").counters().sumOf { it.count() }.toInt()
                    val completedFailedRetry = meterRegistry.find("khrona.executions.completed").tag("outcome", "failed_retry").counters().sumOf { it.count() }.toInt()
                    val completedDeadLettered = meterRegistry.find("khrona.executions.completed").tag("outcome", "dead_lettered").counters().sumOf { it.count() }.toInt()
                    val deadLetterSize = meterRegistry.find("khrona.dead_letter.size").gauge()?.value()?.toInt() ?: 0
                    val storeOps = meterRegistry.find("khrona.store.operation.duration").timers().sumOf { it.count() }.toInt()
                    val forbiddenKeys = setOf("execution_id", "executionId", "correlation_id", "correlationId", "worker_id", "workerId", "lock_key", "lockKey")
                    val leakDetected = meterRegistry.meters.any { meter ->
                        meter.id.tags.any { tag ->
                            tag.key in forbiddenKeys ||
                                tag.value.contains("e2e-corr-memory-1") ||
                                tag.value.contains("Simulated transient error")
                        }
                    }
                    val body = ""${'"'}{
                        "started": ${'$'}started,
                        "completedSuccess": ${'$'}completedSuccess,
                        "completedFailedRetry": ${'$'}completedFailedRetry,
                        "completedDeadLettered": ${'$'}completedDeadLettered,
                        "deadLetterSize": ${'$'}deadLetterSize,
                        "storeOps": ${'$'}storeOps,
                        "unboundedTagLeak": ${'$'}leakDetected
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json, HttpStatusCode.OK)
                }

                post("/trigger") {
                    val corrId = call.request.headers["X-Correlation-Id"] ?: "default-corr"
                    MDC.put("correlationId", corrId)
                    try {
                        this@configureRouting.plugin(KhronaPlugin).scheduler.trigger(
                            "manual-webhook",
                            mapOf("msg" to "hello-from-e2e", "count" to 42L, "ok" to true)
                        )
                    } finally {
                        MDC.remove("correlationId")
                    }
                    call.respondText("Triggered", status = HttpStatusCode.Accepted)
                }

                get("/status") {
                    val cronRegistered = store.getJob("daily-cron") != null
                    val body = ""${'"'}{
                        "onceRuns": ${'$'}{onceRuns.get()},
                        "intervalRuns": ${'$'}{intervalRuns.get()},
                        "manualRuns": ${'$'}{manualRuns.get()},
                        "retrySuccesses": ${'$'}{retrySuccesses.get()},
                        "deadLetterAttempts": ${'$'}{deadLetterAttempts.get()},
                        "lastManualCorrelationId": "${'$'}{lastManualCorrelationId.get()}",
                        "lastManualPayload": "${'$'}{lastManualPayload.get()}",
                        "fencingPassed": ${'$'}{fencingPassed.get()},
                        "cronRegistered": ${'$'}cronRegistered,
                        "managementVerified": ${'$'}{managementVerified.get() && contextVerified.get()},
                        "checkpointShutdownVerified": ${'$'}{checkpointShutdownVerified.get()}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json)
                }
            }
        }
    """.trimIndent()

    private fun jdbcPostgresUseCaseRoutingSource(): String = """
        package com.example.com

        import com.zaxxer.hikari.HikariConfig
        import com.zaxxer.hikari.HikariDataSource
        import io.khrona.core.*
        import io.khrona.ktor.*
        import io.khrona.observability.OpenTelemetryKhronaObserver
        import io.khrona.store.jdbc.JdbcJobStore
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import io.opentelemetry.api.trace.Span
        import io.opentelemetry.sdk.OpenTelemetrySdk
        import io.opentelemetry.sdk.metrics.SdkMeterProvider
        import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader
        import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
        import io.opentelemetry.sdk.trace.SdkTracerProvider
        import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor
        import kotlinx.coroutines.CancellationException
        import kotlinx.coroutines.CompletableDeferred
        import kotlinx.coroutines.Dispatchers
        import kotlinx.coroutines.async
        import kotlinx.coroutines.awaitCancellation
        import kotlinx.coroutines.coroutineScope
        import kotlinx.coroutines.delay
        import kotlinx.coroutines.launch
        import kotlinx.coroutines.runBlocking
        import kotlinx.coroutines.withTimeout
        import java.time.Duration
        import java.time.Instant
        import java.util.UUID
        import java.util.concurrent.atomic.AtomicBoolean
        import java.util.concurrent.atomic.AtomicInteger
        import kotlin.time.Duration.Companion.milliseconds
        import kotlin.time.Duration.Companion.seconds

        fun Application.configureRouting() {
            val hikari = HikariDataSource(HikariConfig().apply {
                jdbcUrl = System.getenv("DB_URL")
                username = System.getenv("DB_USER")
                password = System.getenv("DB_PASSWORD")
                maximumPoolSize = 5
            })
            val spanExporter = InMemorySpanExporter.create()
            val metricReader = InMemoryMetricReader.create()
            val otelSdk = OpenTelemetrySdk.builder()
                .setTracerProvider(
                    SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                        .build()
                )
                .setMeterProvider(
                    SdkMeterProvider.builder()
                        .registerMetricReader(metricReader)
                        .build()
                )
                .build()
            val otelObserver = OpenTelemetryKhronaObserver(otelSdk)
            monitor.subscribe(ApplicationStopped) {
                otelObserver.close()
                otelSdk.close()
                hikari.close()
            }
            val rawStore = JdbcJobStore(hikari)
            val store = rawStore.observedBy(otelObserver)

            val intervalRuns = AtomicInteger(0)
            val otelContextPropagated = AtomicBoolean(false)
            val replaceCount = AtomicInteger(0)
            val replaceCancelledFirst = AtomicBoolean(false)
            val replaceCompletedSecond = AtomicBoolean(false)
            val forbidVerified = AtomicBoolean(false)
            val retentionCleaned = AtomicBoolean(false)
            val fencingVerified = AtomicBoolean(false)
            val managementVerified = AtomicBoolean(false)
            val pgPausedRuns = AtomicInteger(0)
            val pgStopStarted = CompletableDeferred<UUID>()
            val staleMisfire = JobExecution(
                jobId = "jdbc-misfire-ignore",
                scheduledAt = Instant.now().minusSeconds(30)
            )
            val staleMisfireId = staleMisfire.id

            runBlocking {
                rawStore.migrate()
            }

            installKhrona {
                this.store = store
                this.observer = otelObserver
                pollingInterval(100.milliseconds)
                misfireThreshold(2.seconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
                retention {
                    enabled = true
                    cleanupJitter = 0.1
                }
            }

            runBlocking {
                scheduler {
                    pollingInterval(100.milliseconds)

                    job("jdbc-interval") {
                        every(200.milliseconds)
                        execute {
                            val parentSpan = Span.current().spanContext
                            delay(5)
                            val childSpanId = coroutineScope {
                                async(Dispatchers.Default) {
                                    Span.current().spanContext.spanId
                                }.await()
                            }
                            if (parentSpan.isValid && childSpanId == parentSpan.spanId) {
                                otelContextPropagated.set(true)
                            }
                            intervalRuns.incrementAndGet()
                        }
                    }

                    // UC-6 & EC-B3/B4: REPLACE concurrency policy
                    job("jdbc-replace") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        concurrencyPolicy = ConcurrencyPolicy.REPLACE
                        lockKey = "jdbc-replace-lock"
                        execute {
                            val runNumber = replaceCount.incrementAndGet()
                            if (runNumber == 1) {
                                try {
                                    delay(10_000)
                                } catch (e: CancellationException) {
                                    replaceCancelledFirst.set(true)
                                    throw e
                                }
                            } else {
                                delay(100)
                                replaceCompletedSecond.set(true)
                            }
                        }
                    }

                    // UC-11 & EC-C4: Misfire IGNORE policy
                    job("jdbc-misfire-ignore") {
                        every(10.seconds)
                        misfirePolicy = MisfirePolicy.IGNORE
                        execute {}
                    }

                    job("jdbc-managed-pause") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        paused = true
                        execute {
                            pgPausedRuns.incrementAndGet()
                        }
                    }

                    job("jdbc-managed-stop") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        execute {
                            pgStopStarted.complete(executionId)
                            awaitCancellation()
                        }
                    }
                }

                // Enqueue a stale execution for misfire IGNORE verification
                store.saveExecution(staleMisfire)

                // Verify FORBID lock mutual exclusion, startup save race protection, SKIP LOCKED retention cleanup, and fencing on PostgreSQL
                val forbidExec = JobExecution(jobId = "forbid-check", scheduledAt = Instant.now(), lockKey = "pg-shared-lock")
                store.saveExecution(forbidExec)
                val claimed = store.claimExecution(forbidExec.id, "pg-worker-1", Duration.ofMinutes(1))
                store.saveExecution(forbidExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileClaimed = store.getExecution(forbidExec.id)?.status == ExecutionStatus.CLAIMED &&
                    store.getExecution(forbidExec.id)?.workerId == "pg-worker-1"
                val lockHeldForOthers = store.isLockHeld("pg-shared-lock")
                val lockNotHeldForSelf = !store.isLockHeld("pg-shared-lock", excludeExecutionId = forbidExec.id)
                val wrongWorkerStatusRejected = !store.updateExecutionStatus(forbidExec.id, ExecutionStatus.SUCCESS, workerId = "pg-worker-2")
                val rightWorkerStatusAccepted = store.updateExecutionStatus(forbidExec.id, ExecutionStatus.SUCCESS, workerId = "pg-worker-1")
                store.saveExecution(forbidExec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileSuccess = store.getExecution(forbidExec.id)?.status == ExecutionStatus.SUCCESS
                forbidVerified.set(claimed && lockHeldForOthers && lockNotHeldForSelf)
                fencingVerified.set(
                    latePendingIgnoredWhileClaimed && wrongWorkerStatusRejected &&
                        rightWorkerStatusAccepted && latePendingIgnoredWhileSuccess
                )

                // UC-12 & EC-D3: Retention cleanup with FOR UPDATE SKIP LOCKED contention on PostgreSQL
                val lockedExec = JobExecution(jobId = "pg-locked-cleanup", scheduledAt = Instant.now().minusSeconds(10))
                store.saveExecution(lockedExec)
                store.updateExecutionStatus(lockedExec.id, ExecutionStatus.SUCCESS)
                val skipLockedCleanupPassed = hikari.connection.use { lockConn ->
                    lockConn.autoCommit = false
                    try {
                        lockConn.prepareStatement("SELECT id FROM khrona_executions WHERE id = ? FOR UPDATE").use { stmt ->
                            stmt.setString(1, lockedExec.id.toString())
                            stmt.executeQuery().use { it.next() }
                        }
                        val cleaned = store.cleanupCompletedExecutions(
                            before = Instant.now().plusSeconds(60),
                            statuses = setOf(ExecutionStatus.SUCCESS),
                            limit = 1
                        )
                        cleaned == 1 && store.getExecution(forbidExec.id) == null && store.getExecution(lockedExec.id) != null
                    } finally {
                        lockConn.rollback()
                    }
                }
                retentionCleaned.set(skipLockedCleanupPassed)

                // Trigger REPLACE execution #1, then #2 shortly after once ApplicationStarted fires
                val khronaScheduler = this@configureRouting.plugin(KhronaPlugin).scheduler
                this@configureRouting.monitor.subscribe(ApplicationStarted) { app ->
                    app.launch {
                        delay(200)
                        khronaScheduler.trigger("jdbc-replace")
                        delay(400)
                        khronaScheduler.trigger("jdbc-replace")

                        // Verify PostgreSQL pause/resume, listExecutions, getJobOverview, and CANCELLED local stop
                        val queuedPaused = khronaScheduler.startJob("jdbc-managed-pause")
                        val overviewWhilePaused = khronaScheduler.getJobOverview("jdbc-managed-pause")
                        delay(250)
                        val stayedPaused = pgPausedRuns.get() == 0 && overviewWhilePaused?.paused == true
                        khronaScheduler.resumeJob("jdbc-managed-pause")
                        withTimeout(5_000) {
                            while (pgPausedRuns.get() < 1) {
                                delay(25)
                            }
                        }

                        val stopExecId = khronaScheduler.startJob("jdbc-managed-stop").affectedExecutionIds.single()
                        withTimeout(5_000) { pgStopStarted.await() }
                        val stopRes = khronaScheduler.stopExecution(stopExecId)
                        val cancelledRows = khronaScheduler.listExecutions(
                            JobExecutionQuery(
                                jobId = "jdbc-managed-stop",
                                statuses = setOf(ExecutionStatus.CANCELLED)
                            )
                        )
                        val overviewAfterStop = khronaScheduler.getJobOverview("jdbc-managed-stop")
                        managementVerified.set(
                            queuedPaused.success && stayedPaused && pgPausedRuns.get() == 1 &&
                                stopRes.success && cancelledRows.any { it.id == stopExecId && it.completedAt != null } &&
                                overviewAfterStop?.progress?.countsByStatus?.get(ExecutionStatus.CANCELLED) == 1
                        )
                    }
                }
            }

            routing {
                get("/health") {
                    val health = this@configureRouting.plugin(KhronaPlugin).scheduler.health()
                    val code = if (health.isReady) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable
                    val body = ""${'"'}{
                        "status": "${'$'}{health.status.name}",
                        "storeStatus": "${'$'}{health.storeHealth.status.name}",
                        "isHealthy": ${'$'}{health.isHealthy},
                        "isReady": ${'$'}{health.isReady}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json, code)
                }

                get("/otel-summary") {
                    val spans = spanExporter.finishedSpanItems.filter { it.name == "khrona.job.execute" }
                    val metrics = metricReader.collectAllMetrics()
                    val metricsByName = metrics.associateBy { it.name }
                    val completedCount = metricsByName["khrona.executions.completed"]
                        ?.longSumData?.points?.sumOf { it.value } ?: 0L
                    val storeOpCount = metricsByName["khrona.store.operation.duration"]
                        ?.histogramData?.points?.sumOf { it.count } ?: 0L
                    val forbiddenKeys = setOf(
                        "execution_id", "executionId", "khrona.execution.id",
                        "correlation_id", "correlationId", "khrona.correlation_id",
                        "worker_id", "workerId", "lock_key", "lockKey"
                    )
                    val unboundedLeak = metrics.any { metric ->
                        val points = metric.longSumData.points +
                            metric.histogramData.points +
                            metric.longGaugeData.points
                        points.any { point ->
                            point.attributes.asMap().keys.any { it.key in forbiddenKeys }
                        }
                    }
                    val body = ""${'"'}{
                        "otelSpanCount": ${'$'}{spans.size},
                        "otelContextPropagated": ${'$'}{otelContextPropagated.get()},
                        "otelCompletedCount": ${'$'}completedCount,
                        "otelStoreOpCount": ${'$'}storeOpCount,
                        "unboundedMetricAttributeLeak": ${'$'}unboundedLeak
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json)
                }

                get("/status") {
                    val isMisfireIgnored = store.getExecution(staleMisfireId)?.status == ExecutionStatus.MISFIRED
                    val body = ""${'"'}{
                        "intervalRuns": ${'$'}{intervalRuns.get()},
                        "replaceCancelledFirst": ${'$'}{replaceCancelledFirst.get()},
                        "replaceCompletedSecond": ${'$'}{replaceCompletedSecond.get()},
                        "forbidVerified": ${'$'}{forbidVerified.get()},
                        "misfireIgnored": ${'$'}isMisfireIgnored,
                        "retentionCleaned": ${'$'}{retentionCleaned.get()},
                        "fencingVerified": ${'$'}{fencingVerified.get()},
                        "managementVerified": ${'$'}{managementVerified.get()}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json)
                }
            }
        }
    """.trimIndent()

    private fun redisUseCaseRoutingSource(): String = """
        package com.example.com

        import io.khrona.core.*
        import io.khrona.ktor.*
        import io.khrona.store.redis.RedisJobStore
        import io.khrona.store.redis.RedisJobStoreConfig
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import kotlinx.coroutines.CancellationException
        import kotlinx.coroutines.CompletableDeferred
        import kotlinx.coroutines.awaitCancellation
        import kotlinx.coroutines.delay
        import kotlinx.coroutines.launch
        import kotlinx.coroutines.runBlocking
        import kotlinx.coroutines.withTimeout
        import java.time.Duration
        import java.time.Instant
        import java.util.UUID
        import java.util.concurrent.atomic.AtomicBoolean
        import java.util.concurrent.atomic.AtomicInteger
        import kotlin.time.Duration.Companion.milliseconds
        import kotlin.time.Duration.Companion.seconds

        fun Application.configureRouting() {
            val store = RedisJobStore(
                RedisJobStoreConfig(
                    redisUri = System.getenv("REDIS_URI"),
                    namespace = RedisJobStoreConfig.clusterNamespace("khrona-e2e")
                )
            )
            monitor.subscribe(ApplicationStopped) {
                store.close()
            }
            val intervalRuns = AtomicInteger(0)
            val replaceCount = AtomicInteger(0)
            val replaceCancelledFirst = AtomicBoolean(false)
            val replaceCompletedSecond = AtomicBoolean(false)
            val heartbeatExtended = AtomicBoolean(false)
            val staleRecovered = AtomicBoolean(false)
            val replaceSuperseded = AtomicBoolean(false)
            val retentionCleaned = AtomicBoolean(false)
            val fencingVerified = AtomicBoolean(false)
            val managementVerified = AtomicBoolean(false)
            val pgPausedRuns = AtomicInteger(0)
            val pgStopStarted = CompletableDeferred<UUID>()

            installKhrona {
                this.store = store
                pollingInterval(100.milliseconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
                retention {
                    enabled = true
                    cleanupJitter = 0.1
                }
            }

            runBlocking {
                scheduler {
                    pollingInterval(100.milliseconds)

                    job("redis-interval") {
                        every(200.milliseconds)
                        execute {
                            intervalRuns.incrementAndGet()
                        }
                    }

                    // UC-6 & EC-B3/B4: REPLACE concurrency policy over Redis
                    job("redis-replace") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        concurrencyPolicy = ConcurrencyPolicy.REPLACE
                        lockKey = "redis-replace-e2e-lock"
                        execute {
                            val runNumber = replaceCount.incrementAndGet()
                            if (runNumber == 1) {
                                try {
                                    delay(10_000)
                                } catch (e: CancellationException) {
                                    replaceCancelledFirst.set(true)
                                    throw e
                                }
                            } else {
                                delay(100)
                                replaceCompletedSecond.set(true)
                            }
                        }
                    }
                }

                // Verify atomic claiming, startup save race protection, heartbeat renewal, RUNNING status transition preserving lease, and fencing in Redis
                val exec = JobExecution(jobId = "redis-direct", scheduledAt = Instant.now(), lockKey = "redis-lock")
                store.saveExecution(exec)
                val claimed = store.claimExecution(exec.id, "redis-worker-1", Duration.ofSeconds(5))
                store.saveExecution(exec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileClaimed = store.getExecution(exec.id)?.status == ExecutionStatus.CLAIMED &&
                    store.getExecution(exec.id)?.workerId == "redis-worker-1"
                val initialExpiry = store.getExecution(exec.id)?.expiresAt!!
                val runningTransition = store.updateExecutionStatus(exec.id, ExecutionStatus.RUNNING, workerId = "redis-worker-1")
                val hbSuccess = store.heartbeat(exec.id, "redis-worker-1", Duration.ofMinutes(2))
                val extendedExpiry = store.getExecution(exec.id)?.expiresAt!!
                val wrongWorkerHb = !store.heartbeat(exec.id, "redis-worker-2", Duration.ofMinutes(2))
                val wrongWorkerStatus = !store.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS, workerId = "redis-worker-2")
                heartbeatExtended.set(claimed && runningTransition && hbSuccess && extendedExpiry.isAfter(initialExpiry))

                // Supersede by lock key and verify former owner can no longer heartbeat or complete, nor be overwritten by late PENDING save
                val supersededIds = store.supersedeExecutionsByLockKey("redis-lock")
                store.saveExecution(exec.copy(status = ExecutionStatus.PENDING, workerId = null, expiresAt = null))
                val latePendingIgnoredWhileSuperseded = store.getExecution(exec.id)?.status == ExecutionStatus.SUPERSEDED
                val hbAfterSupersede = !store.heartbeat(exec.id, "redis-worker-1", Duration.ofMinutes(2))
                val statusAfterSupersede = !store.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS, workerId = "redis-worker-1")
                replaceSuperseded.set(supersededIds == listOf(exec.id) && store.getExecution(exec.id)?.status == ExecutionStatus.SUPERSEDED)
                fencingVerified.set(
                    latePendingIgnoredWhileClaimed && latePendingIgnoredWhileSuperseded &&
                        wrongWorkerHb && wrongWorkerStatus && hbAfterSupersede && statusAfterSupersede
                )

                // Verify Redis retention cleanup on SUPERSEDED executions
                val cleaned = store.cleanupCompletedExecutions(
                    before = Instant.now().plusSeconds(60),
                    statuses = setOf(ExecutionStatus.SUPERSEDED),
                    limit = 10
                )
                retentionCleaned.set(cleaned >= 1 && store.getExecution(exec.id) == null)

                // Verify atomic resetExpiredExecutions on Redis
                val expiredExec = JobExecution(
                    jobId = "redis-expired",
                    scheduledAt = Instant.now().minusSeconds(30),
                    startedAt = Instant.now().minusSeconds(20),
                    expiresAt = Instant.now().minusSeconds(2),
                    status = ExecutionStatus.RUNNING,
                    workerId = "dead-worker"
                )
                store.saveExecution(expiredExec)
                val recovered = store.resetExpiredExecutions(Instant.now())
                staleRecovered.set(recovered == 1 && store.getExecution(expiredExec.id)?.status == ExecutionStatus.PENDING)

                // Trigger REPLACE execution #1, then #2 shortly after once ApplicationStarted fires
                val khronaScheduler = this@configureRouting.plugin(KhronaPlugin).scheduler
                this@configureRouting.monitor.subscribe(ApplicationStarted) { app ->
                    app.launch {
                        delay(200)
                        khronaScheduler.trigger("redis-replace")
                        delay(400)
                        khronaScheduler.trigger("redis-replace")
                    }
                }
            }

            routing {
                get("/health") {
                    val health = this@configureRouting.plugin(KhronaPlugin).scheduler.health()
                    val code = if (health.isReady) HttpStatusCode.OK else HttpStatusCode.ServiceUnavailable
                    val body = ""${'"'}{
                        "status": "${'$'}{health.status.name}",
                        "storeStatus": "${'$'}{health.storeHealth.status.name}",
                        "isHealthy": ${'$'}{health.isHealthy},
                        "isReady": ${'$'}{health.isReady}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json, code)
                }

                get("/status") {
                    val body = ""${'"'}{
                        "intervalRuns": ${'$'}{intervalRuns.get()},
                        "replaceCancelledFirst": ${'$'}{replaceCancelledFirst.get()},
                        "replaceCompletedSecond": ${'$'}{replaceCompletedSecond.get()},
                        "heartbeatExtended": ${'$'}{heartbeatExtended.get()},
                        "staleRecovered": ${'$'}{staleRecovered.get()},
                        "replaceSuperseded": ${'$'}{replaceSuperseded.get()},
                        "retentionCleaned": ${'$'}{retentionCleaned.get()},
                        "fencingVerified": ${'$'}{fencingVerified.get()},
                        "managementVerified": ${'$'}{managementVerified.get()}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json)
                }
            }
        }
    """.trimIndent()

    private fun httpGet(url: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build()
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun httpPost(url: String, headers: Map<String, String> = emptyMap()): HttpResponse<String> {
        val builder = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.noBody())
        headers.forEach { (k, v) -> builder.header(k, v) }
        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString())
    }

    private fun awaitStatus(
        baseUrl: String,
        timeout: Duration = Duration.ofSeconds(30),
        predicate: (JsonObject) -> Boolean
    ): JsonObject = awaitHttpJson("$baseUrl/status", expectedStatus = 200, timeout = timeout, predicate = predicate)

    private fun awaitJson(
        url: String,
        timeout: Duration = Duration.ofSeconds(15),
        predicate: (JsonObject) -> Boolean
    ): JsonObject = awaitHttpJson(url, expectedStatus = 200, timeout = timeout, predicate = predicate)

    private fun awaitHttpJson(
        url: String,
        expectedStatus: Int,
        timeout: Duration = Duration.ofSeconds(15),
        predicate: (JsonObject) -> Boolean
    ): JsonObject {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        var lastStatus: Int? = null
        var lastBody: String? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                val response = httpGet(url)
                lastStatus = response.statusCode()
                lastBody = response.body()
                if (lastStatus == expectedStatus) {
                    val json = Json.parseToJsonElement(lastBody) as JsonObject
                    if (predicate(json)) {
                        return json
                    }
                }
            } catch (_: Exception) {
                // Ignore transient errors while polling
            }
            Thread.sleep(150)
        }
        throw AssertionError("Timed out waiting for $url status=$expectedStatus. Last status=$lastStatus body=$lastBody")
    }
}
