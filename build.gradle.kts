plugins {
    kotlin("jvm") version "2.3.21"
    `maven-publish`
}

group = "io.github.damian-rafael-lattenero"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    api("io.reactivex.rxjava3:rxjava:3.1.12")
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
