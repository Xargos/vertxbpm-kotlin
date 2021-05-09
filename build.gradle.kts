import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.5.0"
}
group = "process.engine"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    jcenter()
    maven {
        url = uri("https://dl.bintray.com/kotlin/exposed")
    }
}
sourceSets.main {
    java.srcDirs("src/main/java", "src/main/kotlin")
}
dependencies {

//    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk11")
    implementation("com.beust:klaxon:5.0.1")
//    implementation "ch.qos.logback:logback-classic:1.2.3"
//    implementation "ch.qos.logback.contrib:logback-json-classic:0.1.5"
//    implementation "ch.qos.logback.contrib:logback-jackson:0.1.5"
    implementation("io.vertx", "vertx-ignite", "4.0.3")
    implementation("io.vertx", "vertx-web", "4.0.3")
    implementation("io.vertx", "vertx-core", "4.0.3")
    implementation("io.vertx", "vertx-web-client", "4.0.3")
    implementation("de.huxhorn.sulky", "de.huxhorn.sulky.ulid", "8.2.0")
    implementation("io.vavr", "vavr", "1.0.0-alpha-3")
    testImplementation("org.assertj", "assertj-core", "3.14.0")
    testImplementation("io.vertx", "vertx-junit5", "4.0.3")
    testImplementation("io.mockk:mockk:1.9.3")
}
tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}