plugins {
    `java-library`
    `java-test-fixtures`
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.coroutines.slf4j)
    api(libs.kotlinx.serialization.json)
    api(libs.cron.utils)
    implementation(libs.slf4j.api)

    testFixturesApi(libs.junit.jupiter)
    testFixturesApi(libs.kotlinx.coroutines.test)
    
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.logback.classic)
    testImplementation(libs.junit.jupiter)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
