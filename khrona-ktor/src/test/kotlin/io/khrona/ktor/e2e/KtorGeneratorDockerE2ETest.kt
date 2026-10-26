package io.khrona.ktor.e2e

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
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

            // Poll /status until interval, once, cron registration, manual trigger, and retry-to-success all complete
            val status = awaitStatus(baseUrl) { json ->
                json["onceRuns"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["intervalRuns"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                    json["manualRuns"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["retrySuccesses"]?.jsonPrimitive?.int ?: 0 >= 1 &&
                    json["deadLetterAttempts"]?.jsonPrimitive?.int ?: 0 >= 2 &&
                    json["fencingPassed"]?.jsonPrimitive?.boolean == true &&
                    json["cronRegistered"]?.jsonPrimitive?.boolean == true
            }

            assertEquals("e2e-corr-memory-1", status["lastManualCorrelationId"]?.jsonPrimitive?.content)
            assertEquals("hello-from-e2e:42:true", status["lastManualPayload"]?.jsonPrimitive?.content)
            assertEquals(true, status["fencingPassed"]?.jsonPrimitive?.boolean)
        }
    }

    @Test
    fun `scaffolds Ktor project inside Docker with JdbcJobStore and PostgreSQL and validates persistence FORBID REPLACE misfires and retention`() {
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
                                    json["fencingVerified"]?.jsonPrimitive?.boolean == true
                            }

                            assertTrue(status["replaceCancelledFirst"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["replaceCompletedSecond"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["forbidVerified"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["misfireIgnored"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["retentionCleaned"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["fencingVerified"]?.jsonPrimitive?.boolean == true)
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
                                    json["heartbeatExtended"]?.jsonPrimitive?.boolean == true &&
                                    json["staleRecovered"]?.jsonPrimitive?.boolean == true &&
                                    json["replaceSuperseded"]?.jsonPrimitive?.boolean == true &&
                                    json["fencingVerified"]?.jsonPrimitive?.boolean == true
                            }

                            assertTrue(status["heartbeatExtended"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["staleRecovered"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["replaceSuperseded"]?.jsonPrimitive?.boolean == true)
                            assertTrue(status["fencingVerified"]?.jsonPrimitive?.boolean == true)
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
        val gradleUserHome: Path = Paths.get(System.getProperty("user.home"), ".gradle")
        val containerGradleHome: Path = gradleUserHome.resolve("khrona-e2e-container-home")
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
        import io.khrona.store.memory.MemoryJobStore
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.request.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import kotlinx.coroutines.runBlocking
        import org.slf4j.MDC
        import java.time.Duration
        import java.time.Instant
        import java.time.ZoneId
        import java.util.concurrent.atomic.AtomicBoolean
        import java.util.concurrent.atomic.AtomicInteger
        import java.util.concurrent.atomic.AtomicReference
        import kotlin.time.Duration.Companion.milliseconds
        import kotlin.time.Duration.Companion.seconds

        fun Application.configureRouting() {
            val store = MemoryJobStore()
            val onceRuns = AtomicInteger(0)
            val intervalRuns = AtomicInteger(0)
            val manualRuns = AtomicInteger(0)
            val retryAttempts = AtomicInteger(0)
            val retrySuccesses = AtomicInteger(0)
            val deadLetterAttempts = AtomicInteger(0)
            val lastManualCorrelationId = AtomicReference("none")
            val lastManualPayload = AtomicReference("none")
            val fencingPassed = AtomicBoolean(false)

            installKhrona {
                this.store = store
                pollingInterval(100.milliseconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
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

                    // UC-4: On-demand manual trigger with structured payload & MDC correlationId
                    job("manual-webhook") {
                        at(Instant.now().plus(Duration.ofDays(365)))
                        concurrencyPolicy = ConcurrencyPolicy.ALLOW
                        execute { payload ->
                            @Suppress("UNCHECKED_CAST")
                            val map = payload as? Map<String, Any?>
                            lastManualPayload.set("${'$'}{map?.get("msg")}:${'$'}{map?.get("count")}:${'$'}{map?.get("ok")}")
                            lastManualCorrelationId.set(MDC.get("correlationId") ?: "missing")
                            manualRuns.incrementAndGet()
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

                // EC-A3 & EC-A5: Verify heartbeat & status transition fencing directly against the store
                val fencedExec = JobExecution(jobId = "fenced-direct", scheduledAt = Instant.now())
                store.saveExecution(fencedExec)
                val claimed1 = store.claimExecution(fencedExec.id, "worker-1", Duration.ofSeconds(-5))
                val expiredHbRejected = !store.heartbeat(fencedExec.id, "worker-1", Duration.ofMinutes(1))
                val claimed2 = store.claimExecution(fencedExec.id, "worker-2", Duration.ofMinutes(1))
                val staleHbRejected = !store.heartbeat(fencedExec.id, "worker-1", Duration.ofMinutes(1))
                val staleStatusRejected = !store.updateExecutionStatus(fencedExec.id, ExecutionStatus.SUCCESS, workerId = "worker-1")
                val ownerStatusAccepted = store.updateExecutionStatus(fencedExec.id, ExecutionStatus.SUCCESS, workerId = "worker-2")
                fencingPassed.set(claimed1 && expiredHbRejected && claimed2 && staleHbRejected && staleStatusRejected && ownerStatusAccepted)
            }

            routing {
                get("/health") {
                    call.respondText("OK", ContentType.Text.Plain, HttpStatusCode.OK)
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
                        "cronRegistered": ${'$'}cronRegistered
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
        import io.khrona.store.jdbc.JdbcJobStore
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import kotlinx.coroutines.CancellationException
        import kotlinx.coroutines.delay
        import kotlinx.coroutines.launch
        import kotlinx.coroutines.runBlocking
        import java.time.Duration
        import java.time.Instant
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
            val store = JdbcJobStore(hikari)

            val intervalRuns = AtomicInteger(0)
            val replaceCount = AtomicInteger(0)
            val replaceCancelledFirst = AtomicBoolean(false)
            val replaceCompletedSecond = AtomicBoolean(false)
            val forbidVerified = AtomicBoolean(false)
            val retentionCleaned = AtomicBoolean(false)
            val fencingVerified = AtomicBoolean(false)
            val staleMisfire = JobExecution(
                jobId = "jdbc-misfire-ignore",
                scheduledAt = Instant.now().minusSeconds(30)
            )
            val staleMisfireId = staleMisfire.id

            runBlocking {
                store.migrate()
            }

            installKhrona {
                this.store = store
                pollingInterval(100.milliseconds)
                misfireThreshold(2.seconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
            }

            runBlocking {
                scheduler {
                    pollingInterval(100.milliseconds)

                    job("jdbc-interval") {
                        every(200.milliseconds)
                        execute {
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
                }

                // Enqueue a stale execution for misfire IGNORE verification
                store.saveExecution(staleMisfire)

                // Verify FORBID lock mutual exclusion, retention cleanup, and fencing on PostgreSQL
                val forbidExec = JobExecution(jobId = "forbid-check", scheduledAt = Instant.now(), lockKey = "pg-shared-lock")
                store.saveExecution(forbidExec)
                val claimed = store.claimExecution(forbidExec.id, "pg-worker-1", Duration.ofMinutes(1))
                val lockHeldForOthers = store.isLockHeld("pg-shared-lock")
                val lockNotHeldForSelf = !store.isLockHeld("pg-shared-lock", excludeExecutionId = forbidExec.id)
                val wrongWorkerStatusRejected = !store.updateExecutionStatus(forbidExec.id, ExecutionStatus.SUCCESS, workerId = "pg-worker-2")
                val rightWorkerStatusAccepted = store.updateExecutionStatus(forbidExec.id, ExecutionStatus.SUCCESS, workerId = "pg-worker-1")
                forbidVerified.set(claimed && lockHeldForOthers && lockNotHeldForSelf)
                fencingVerified.set(wrongWorkerStatusRejected && rightWorkerStatusAccepted)

                // UC-12 & EC-D3: Retention cleanup on PostgreSQL
                val cleaned = store.cleanupCompletedExecutions(
                    before = Instant.now().plusSeconds(60),
                    statuses = setOf(ExecutionStatus.SUCCESS),
                    limit = 100
                )
                retentionCleaned.set(cleaned >= 1 && store.getExecution(forbidExec.id) == null)

                // Trigger REPLACE execution #1, then #2 shortly after once ApplicationStarted fires
                val khronaScheduler = this@configureRouting.plugin(KhronaPlugin).scheduler
                this@configureRouting.monitor.subscribe(ApplicationStarted) { app ->
                    app.launch {
                        delay(200)
                        khronaScheduler.trigger("jdbc-replace")
                        delay(400)
                        khronaScheduler.trigger("jdbc-replace")
                    }
                }
            }

            routing {
                get("/health") {
                    call.respondText("OK", ContentType.Text.Plain, HttpStatusCode.OK)
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
                        "fencingVerified": ${'$'}{fencingVerified.get()}
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
        import io.ktor.http.*
        import io.ktor.server.application.*
        import io.ktor.server.response.*
        import io.ktor.server.routing.*
        import kotlinx.coroutines.runBlocking
        import java.time.Duration
        import java.time.Instant
        import java.util.concurrent.atomic.AtomicBoolean
        import java.util.concurrent.atomic.AtomicInteger
        import kotlin.time.Duration.Companion.milliseconds
        import kotlin.time.Duration.Companion.seconds

        fun Application.configureRouting() {
            val store = RedisJobStore(System.getenv("REDIS_URI"), namespace = "khrona-e2e")
            val intervalRuns = AtomicInteger(0)
            val heartbeatExtended = AtomicBoolean(false)
            val staleRecovered = AtomicBoolean(false)
            val replaceSuperseded = AtomicBoolean(false)
            val fencingVerified = AtomicBoolean(false)

            installKhrona {
                this.store = store
                pollingInterval(100.milliseconds)
                executionLeaseDuration(5.seconds)
                heartbeatInterval(1.seconds)
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
                }

                // Verify atomic claiming, heartbeat renewal, RUNNING status transition preserving lease, and fencing in Redis
                val exec = JobExecution(jobId = "redis-direct", scheduledAt = Instant.now(), lockKey = "redis-lock")
                store.saveExecution(exec)
                val claimed = store.claimExecution(exec.id, "redis-worker-1", Duration.ofSeconds(5))
                val initialExpiry = store.getExecution(exec.id)?.expiresAt!!
                val runningTransition = store.updateExecutionStatus(exec.id, ExecutionStatus.RUNNING, workerId = "redis-worker-1")
                val hbSuccess = store.heartbeat(exec.id, "redis-worker-1", Duration.ofMinutes(2))
                val extendedExpiry = store.getExecution(exec.id)?.expiresAt!!
                val wrongWorkerHb = !store.heartbeat(exec.id, "redis-worker-2", Duration.ofMinutes(2))
                val wrongWorkerStatus = !store.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS, workerId = "redis-worker-2")
                heartbeatExtended.set(claimed && runningTransition && hbSuccess && extendedExpiry.isAfter(initialExpiry))

                // Supersede by lock key and verify former owner can no longer heartbeat or complete
                val supersededIds = store.supersedeExecutionsByLockKey("redis-lock")
                val hbAfterSupersede = !store.heartbeat(exec.id, "redis-worker-1", Duration.ofMinutes(2))
                val statusAfterSupersede = !store.updateExecutionStatus(exec.id, ExecutionStatus.SUCCESS, workerId = "redis-worker-1")
                replaceSuperseded.set(supersededIds == listOf(exec.id) && store.getExecution(exec.id)?.status == ExecutionStatus.SUPERSEDED)
                fencingVerified.set(wrongWorkerHb && wrongWorkerStatus && hbAfterSupersede && statusAfterSupersede)

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
            }

            routing {
                get("/health") {
                    call.respondText("OK", ContentType.Text.Plain, HttpStatusCode.OK)
                }

                get("/status") {
                    val body = ""${'"'}{
                        "intervalRuns": ${'$'}{intervalRuns.get()},
                        "heartbeatExtended": ${'$'}{heartbeatExtended.get()},
                        "staleRecovered": ${'$'}{staleRecovered.get()},
                        "replaceSuperseded": ${'$'}{replaceSuperseded.get()},
                        "fencingVerified": ${'$'}{fencingVerified.get()}
                    }""${'"'}.trimIndent()
                    call.respondText(body, ContentType.Application.Json)
                }
            }
        }
    """.trimIndent()

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
    ): JsonObject {
        val deadline = System.currentTimeMillis() + timeout.toMillis()
        var lastJson: JsonObject? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                val request = HttpRequest.newBuilder()
                    .uri(URI.create("$baseUrl/status"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build()
                val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
                if (response.statusCode() == 200) {
                    val json = Json.parseToJsonElement(response.body()) as JsonObject
                    lastJson = json
                    if (predicate(json)) {
                        return json
                    }
                }
            } catch (_: Exception) {
                // Ignore transient errors while polling
            }
            Thread.sleep(250)
        }
        throw AssertionError("Timed out waiting for /status condition. Last status: $lastJson")
    }
}
