import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
    `maven-publish`
}

group = "network.arca"
// JitPack injects the git tag as the VERSION env var when it builds the mirror
// repo (arca-kotlin-sdk), so the published Maven coordinate matches the tag
// (e.g. `com.github.arcaresearch:arca-kotlin-sdk:v0.1.0`). Falls back to a dev
// version for local builds.
version = System.getenv("VERSION") ?: "0.1.0"

// The library targets JVM 1.8 bytecode so it is consumable from Android
// projects (minSdk 24+) and any JVM 8+ runtime. We compile with whatever
// JDK runs Gradle rather than pinning a toolchain, since CI/dev only need
// a JDK >= 17 to drive the build.
kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_1_8)
    }
    explicitApi()
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
    withSourcesJar()
}

dependencies {
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.serialization.json)
    api(libs.okhttp)

    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.okhttp.mockwebserver)
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "arca-sdk"
            from(components["java"])
            pom {
                name.set("Arca SDK")
                description.set("Kotlin/Android SDK for the Arca financial infrastructure platform")
                url.set("https://github.com/arcaresearch/arca-kotlin-sdk")
                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }
            }
        }
    }
}
