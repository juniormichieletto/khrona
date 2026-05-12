plugins {
    `java-library`
}

dependencies {
    api(project(":khrona-core"))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.slf4j.simple)
    testImplementation(testFixtures(project(":khrona-core")))
    "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
}
