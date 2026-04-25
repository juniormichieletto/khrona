plugins {
    id("com.gradleup.nmcp.settings") version "1.4.4"
}

rootProject.name = "khrona"

include("khrona-core")
include("khrona-ktor")
include("khrona-store-memory")
include("khrona-store-jdbc")

nmcpSettings {
    centralPortal {
        username.set(providers.environmentVariable("OSSRH_USERNAME").getOrNull())
        password.set(providers.environmentVariable("OSSRH_PASSWORD").getOrNull())
    }
}