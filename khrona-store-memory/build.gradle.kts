dependencies {
    implementation(project(":khrona-core"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
