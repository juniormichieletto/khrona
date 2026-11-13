plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    jacoco
    id("org.jreleaser") version "1.24.0"
    id("signing")
    id("maven-publish")
}

val releaseVersion = providers.gradleProperty("releaseVersion").orElse("0.6.0")

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
        toolVersion = "0.8.15"
    }

    configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
            freeCompilerArgs.addAll("-Xjsr305=strict", "-Xjdk-release=21")
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
    }

    tasks.named<Test>("test") {
        useJUnitPlatform {
            excludeTags("e2e", "perf")
        }
    }

    tasks.register<Test>("perfTest") {
        group = LifecycleBasePlugin.VERIFICATION_GROUP
        description = "Runs scheduler CPU and memory polling performance benchmark tests."
        val testSourceSet = project.extensions.getByType<SourceSetContainer>()["test"]
        testClassesDirs = testSourceSet.output.classesDirs
        classpath = testSourceSet.runtimeClasspath
        useJUnitPlatform {
            includeTags("perf")
        }
        testLogging {
            showStandardStreams = true
        }
        maxParallelForks = 1
        systemProperty("junit.jupiter.execution.parallel.enabled", "false")
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

    tasks.named<org.gradle.testing.jacoco.tasks.JacocoCoverageVerification>("jacocoTestCoverageVerification") {
        dependsOn(tasks.named<Test>("test"))
        sourceDirectories.setFrom(layout.projectDirectory.dir("src/main/kotlin"))
        classDirectories.setFrom(layout.buildDirectory.dir("classes/kotlin/main"))
        violationRules {
            rule {
                limit {
                    counter = "LINE"
                    value = "COVEREDRATIO"
                    minimum = "0.90".toBigDecimal()
                }
                limit {
                    counter = "BRANCH"
                    value = "COVEREDRATIO"
                    minimum = "0.85".toBigDecimal()
                }
            }
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

    if (project.name in setOf("khrona-core", "khrona-ktor", "khrona-store-memory", "khrona-store-jdbc")) {
        val pitestCli = configurations.create("pitestCli")
        dependencies {
            add("pitestCli", "org.pitest:pitest-command-line:1.25.9")
            add("pitestCli", "org.pitest:pitest-junit5-plugin:1.2.3")
        }

        tasks.register<JavaExec>("pitest") {
            group = LifecycleBasePlugin.VERIFICATION_GROUP
            description = "Runs Pitest mutation testing on unit and in-memory tests (excluding E2E and Docker container tests)."
            dependsOn(tasks.named("testClasses"))
            val testSourceSet = project.extensions.getByType<SourceSetContainer>()["test"]
            classpath = pitestCli + testSourceSet.runtimeClasspath
            mainClass.set("org.pitest.mutationtest.commandline.MutationCoverageReport")
            val reportDir = layout.buildDirectory.dir("reports/pitest").get().asFile
            val mainClassesDir = layout.buildDirectory.dir("classes/kotlin/main").get().asFile
            val sourceDir = layout.projectDirectory.dir("src/main/kotlin").asFile
            outputs.dir(reportDir)
            doFirst {
                val ownJar = layout.buildDirectory.file("libs/${project.name}-${project.version}.jar").get().asFile
                val cpEntries = (listOf(mainClassesDir) + (pitestCli + testSourceSet.runtimeClasspath).files)
                    .filter { it.exists() && it != ownJar }
                    .distinct()
                val cpFiles = cpEntries.joinToString(",") { it.absolutePath }
                args(
                    "--reportDir", reportDir.absolutePath,
                    "--classPath", cpFiles,
                    "--mutableCodePaths", mainClassesDir.absolutePath,
                    "--targetClasses", "io.khrona.*",
                    "--targetTests", "io.khrona.*",
                    "--excludedClasses", "*\$serializer,*\$Companion*,*\$\$inlined\$*",
                    "--excludedTestClasses", "*PerfTest,*E2ETest,*EndToEndTest,*PostgreSqlJobStoreTest,*MySqlJobStoreTest,*OracleJobStoreTest",
                    "--excludedGroups", "e2e,perf",
                    "--excludedMethods", "hashCode,equals,toString,copy,copy\$default,write\$Self,component1,component2,component3,component4,component5,component6,component7,component8,component9,component10,component11,component12,component13",
                    "--mutators", "CONDITIONALS_BOUNDARY,INCREMENTS,INVERT_NEGS,MATH,NEGATE_CONDITIONALS,EMPTY_RETURNS,FALSE_RETURNS,TRUE_RETURNS,PRIMITIVE_RETURNS",
                    "--avoidCallsTo", "kotlin.jvm.internal.Intrinsics,kotlin.ResultKt,kotlin.coroutines.intrinsics.IntrinsicsKt,kotlin.jdk7.AutoCloseableKt,kotlin.io.CloseableKt,org.slf4j.Logger",
                    "--timeoutConst", "1500",
                    "--sourceDirs", sourceDir.absolutePath,
                    "--outputFormats", "XML,HTML",
                    "--mutationThreshold", "90",
                    "--timestampedReports=false",
                    "--failWhenNoMutations=false",
                    "--threads", "4"
                )
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

tasks.register<org.gradle.testing.jacoco.tasks.JacocoCoverageVerification>("jacocoRootCoverageVerification") {
    group = LifecycleBasePlugin.VERIFICATION_GROUP
    description = "Verifies aggregate JaCoCo coverage thresholds across all Khrona modules."

    dependsOn(tasks.named("jacocoRootReport"))

    executionData.from(subprojects.map { it.layout.buildDirectory.file("jacoco/test.exec") })
    sourceDirectories.from(subprojects.map { it.layout.projectDirectory.dir("src/main/kotlin") })
    classDirectories.from(subprojects.map { it.layout.buildDirectory.dir("classes/kotlin/main") })

    violationRules {
        rule {
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.95".toBigDecimal()
            }
            limit {
                counter = "INSTRUCTION"
                value = "COVEREDRATIO"
                minimum = "0.90".toBigDecimal()
            }
            limit {
                counter = "BRANCH"
                value = "COVEREDRATIO"
                minimum = "0.88".toBigDecimal()
            }
        }
    }
}
