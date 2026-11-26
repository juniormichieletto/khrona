import java.io.File
import java.util.Properties

plugins {
    `java-library`
}

val micrometerConsumerClasspath = configurations.create("micrometerConsumerClasspath") {
    isCanBeConsumed = false
    isCanBeResolved = true
}

val openTelemetryConsumerClasspath = configurations.create("openTelemetryConsumerClasspath") {
    isCanBeConsumed = false
    isCanBeResolved = true
}

dependencies {
    api(project(":khrona-core"))

    compileOnly(libs.micrometer.core)
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.opentelemetry.extension.kotlin)

    micrometerConsumerClasspath(project(":khrona-observability"))
    micrometerConsumerClasspath(project(":khrona-store-memory"))
    micrometerConsumerClasspath(libs.micrometer.core)

    openTelemetryConsumerClasspath(project(":khrona-observability"))
    openTelemetryConsumerClasspath(project(":khrona-store-memory"))
    openTelemetryConsumerClasspath(libs.opentelemetry.api)
    openTelemetryConsumerClasspath(libs.opentelemetry.extension.kotlin)
    openTelemetryConsumerClasspath(libs.opentelemetry.sdk.testing)

    testImplementation(project(":khrona-store-memory"))
    testImplementation(libs.micrometer.core)
    testImplementation(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.extension.kotlin)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}

val generatedConsumerClasspathDir = layout.buildDirectory.dir("generated/consumer-classpath")

val generateConsumerClasspathProperties = tasks.register("generateConsumerClasspathProperties") {
    inputs.files(micrometerConsumerClasspath)
    inputs.files(openTelemetryConsumerClasspath)
    outputs.dir(generatedConsumerClasspathDir)
    doLast {
        val outDir = generatedConsumerClasspathDir.get().asFile
        outDir.mkdirs()
        val props = Properties().apply {
            setProperty(
                "micrometer.classpath",
                micrometerConsumerClasspath.files.joinToString(File.pathSeparator) { it.absolutePath }
            )
            setProperty(
                "opentelemetry.classpath",
                openTelemetryConsumerClasspath.files.joinToString(File.pathSeparator) { it.absolutePath }
            )
        }
        outDir.resolve("consumer-classpath.properties").outputStream().use { output ->
            props.store(output, "Resolved isolated consumer classpaths for ConsumerClasspathTest")
        }
    }
}

sourceSets["test"].resources.srcDir(generateConsumerClasspathProperties)
