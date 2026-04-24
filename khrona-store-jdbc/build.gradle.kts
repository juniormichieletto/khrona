plugins {
    `java-library`
    kotlin("jvm")
}

dependencies {
    api(project(":khrona-core"))
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.h2)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
