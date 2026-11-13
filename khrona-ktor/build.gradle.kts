plugins {
    `java-library`
}

dependencies {
    api(project(":khrona-core"))
    implementation(libs.ktor.server.core)
    
    testImplementation(project(":khrona-store-memory"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.kotlinx.serialization.json)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}

tasks.register<Test>("e2eTest") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Runs Dockerized Ktor project-generator end-to-end tests against locally published Khrona artifacts."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("e2e")
    }
    testLogging {
        showStandardStreams = true
    }
    maxParallelForks = 1
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    systemProperty("khrona.version", project.version.toString())
    dependsOn(
        ":khrona-core:publishToMavenLocal",
        ":khrona-ktor:publishToMavenLocal",
        ":khrona-store-memory:publishToMavenLocal",
        ":khrona-store-jdbc:publishToMavenLocal",
        ":khrona-store-redis:publishToMavenLocal"
    )
}
