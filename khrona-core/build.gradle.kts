plugins {
    `java-library`
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.serialization.json)
    implementation(libs.slf4j.api)
    
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.junit.jupiter)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
