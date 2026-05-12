plugins {
    `java-library`
    kotlin("jvm")
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    api(project(":khrona-core"))
    implementation(libs.lettuce.core)
    implementation(libs.kotlinx.serialization.json)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(testFixtures(project(":khrona-core")))
    testImplementation(libs.testcontainers.junit)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
