plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    id("signing")
    id("maven-publish")
}

allprojects {
    group = "io.github.juniormichieletto"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
            freeCompilerArgs.add("-Xjsr305=strict")
        }
    }

    tasks.named<Test>("test") {
        useJUnitPlatform()
    }

    configure<PublishingExtension> {
        publications {
            create<MavenPublication>("mavenJava") {
                from(components["java"])
                pom {
                    name.set("Khrona")
                    description.set("Coroutine-native job scheduling for Kotlin and Ktor")
                    url.set("https://github.com/juniormichieletto/khrona")
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("juniormichieletto")
                            name.set("AJ")
                            email.set("juniormichieletto@gmail.com")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com/juniormichieletto/khrona.git")
                        developerConnection.set("scm:git:ssh://github.com:juniormichieletto/khrona.git")
                        url.set("https://github.com/juniormichieletto/khrona")
                    }
                }
            }
        }
    }

    configure<SigningExtension> {
        // Use providers to safely read environment variables
        val signingKeyBase64 = providers.environmentVariable("GPG_PRIVATE_KEY").getOrNull()
        val signingPassword = providers.environmentVariable("GPG_PASSPHRASE").getOrNull()

        if (!signingKeyBase64.isNullOrBlank()) {
            val signingKey = String(java.util.Base64.getDecoder().decode(signingKeyBase64.trim()))
            useInMemoryPgpKeys(signingKey, signingPassword)
            sign(extensions.getByType<PublishingExtension>().publications["mavenJava"])
        } else {
            val isPublishing = gradle.startParameter.taskNames.any { it.contains("publish", ignoreCase = true) }
            
            // Only warn if we are specifically trying to publish and the key is missing
            if (isPublishing) {
                logger.warn("⚠️ GPG_PRIVATE_KEY is missing! Artifacts will NOT be signed and Sonatype will reject them.")
            }
        }
    }
}
