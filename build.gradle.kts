plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    jacoco
    id("org.jreleaser") version "1.24.0"
    id("signing")
    id("maven-publish")
}

val releaseVersion = providers.gradleProperty("releaseVersion").orElse("0.5.0")

allprojects {
    group = "io.github.juniormichieletto"
    version = releaseVersion.get()

    repositories {
        mavenCentral()
    }
}

jreleaser {
    val jreleaserConfigFile = providers.gradleProperty("jreleaserConfigFile").orElse("jreleaser.yml")
    configFile.set(layout.projectDirectory.file(jreleaserConfigFile.get()))
    dependsOnAssemble.set(false)
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "jacoco")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    configure<JacocoPluginExtension> {
        toolVersion = "0.8.13"
    }

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

    tasks.named<org.gradle.testing.jacoco.tasks.JacocoReport>("jacocoTestReport") {
        dependsOn(tasks.named<Test>("test"))

        sourceDirectories.setFrom(layout.projectDirectory.dir("src/main/kotlin"))
        classDirectories.setFrom(layout.buildDirectory.dir("classes/kotlin/main"))

        reports {
            xml.required.set(true)
            html.required.set(true)
            csv.required.set(false)
        }
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

tasks.register<org.gradle.testing.jacoco.tasks.JacocoReport>("jacocoRootReport") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Generates an aggregate JaCoCo coverage report for all Khrona modules."

    dependsOn(subprojects.map { "${it.path}:test" })

    executionData.from(subprojects.map { it.layout.buildDirectory.file("jacoco/test.exec") })
    sourceDirectories.from(subprojects.map { it.layout.projectDirectory.dir("src/main/kotlin") })
    classDirectories.from(subprojects.map { it.layout.buildDirectory.dir("classes/kotlin/main") })

    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}
