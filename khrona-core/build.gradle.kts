plugins {
    `java-library`
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    implementation(libs.slf4j.api)
    
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.junit.jupiter)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
