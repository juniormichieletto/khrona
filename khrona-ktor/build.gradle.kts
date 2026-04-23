plugins {
    `java-library`
}

dependencies {
    api(project(":khrona-core"))
    implementation(libs.ktor.server.core)
    
    testImplementation(project(":khrona-store-memory"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.junit.jupiter)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
