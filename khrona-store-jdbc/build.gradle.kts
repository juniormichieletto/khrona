plugins {
    `java-library`
    kotlin("jvm")
}

dependencies {
    api(project(":khrona-core"))
    compileOnly(libs.postgresql)
    compileOnly(libs.mysql)
    compileOnly(libs.oracle)
    implementation(libs.hikaricp)
    
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.h2)
    testImplementation(testFixtures(project(":khrona-core")))
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.mysql)
    testImplementation(libs.testcontainers.oracle)
    testImplementation(libs.testcontainers.junit)
    testRuntimeOnly(libs.postgresql)
    testRuntimeOnly(libs.mysql)
    testRuntimeOnly(libs.oracle)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test>().configureEach {
    // JDBC tests start heavyweight database containers; keep them serialized to avoid memory spikes.
    maxParallelForks = 1
    systemProperty("junit.jupiter.execution.parallel.enabled", "false")
    mustRunAfter(
        rootProject.subprojects
            .filter { it.path != project.path }
            .map { "${it.path}:test" }
    )
}
