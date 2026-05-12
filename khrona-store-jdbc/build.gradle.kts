plugins {
    `java-library`
    kotlin("jvm")
}

dependencies {
    api(project(":khrona-core"))
    implementation(libs.postgresql)
    implementation(libs.mysql)
    implementation(libs.oracle)
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
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
