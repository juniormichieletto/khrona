plugins {
    `java-library`
}

dependencies {
    api(project(":khrona-core"))

    compileOnly(libs.micrometer.core)
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.opentelemetry.extension.kotlin)

    testImplementation(project(":khrona-store-memory"))
    testImplementation(libs.micrometer.core)
    testImplementation(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.extension.kotlin)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
