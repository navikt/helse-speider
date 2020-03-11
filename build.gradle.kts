import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val junitJupiterVersion = "5.4.0"
val mainClass = "no.nav.helse.speider.AppKt"

plugins {
    kotlin("jvm") version "1.3.60"
}

buildscript {
    dependencies {
        classpath("org.junit.platform:junit-platform-gradle-plugin:1.2.0")
    }
}

val githubUser: String by project
val githubPassword: String by project

dependencies {
    implementation("no.nav.helse:rapids-and-rivers:1.ec0400d")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitJupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$junitJupiterVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitJupiterVersion")
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/navikt/rapids-and-rivers")
        credentials {
            username = githubUser
            password = githubPassword
        }
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_12
    targetCompatibility = JavaVersion.VERSION_12
}

tasks.named<Jar>("jar") {
    archiveBaseName.set("app")

    manifest {
        attributes["Main-Class"] = mainClass
        attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
            it.name
        }
    }

    doLast {
        configurations.runtimeClasspath.get().forEach {
            val file = File("$buildDir/libs/${it.name}")
            if (!file.exists())
                it.copyTo(file)
        }
    }
}

tasks.named<KotlinCompile>("compileKotlin") {
    kotlinOptions.jvmTarget = "12"
}

tasks.named<KotlinCompile>("compileTestKotlin") {
    kotlinOptions.jvmTarget = "12"
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<Wrapper> {
    gradleVersion = "6.1"
}
