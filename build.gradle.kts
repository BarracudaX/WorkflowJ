import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    id("java")
    id("io.freefair.lombok") version "9.5.0"
    id("org.springframework.boot") version "4.0.6"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "com.barracuda"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    //spring
    implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
    implementation("org.springframework.boot:spring-boot-docker-compose")
    implementation("org.springframework.boot:spring-boot-starter-liquibase")

    implementation("org.apache.fory:fory-core:1.3.0")
    implementation("org.mapstruct:mapstruct:1.6.3")
    implementation("org.slf4j:slf4j-api:2.0.18")

    runtimeOnly("org.postgresql:postgresql")

    testImplementation("org.awaitility:awaitility:4.3.0")
    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly ("org.junit.platform:junit-platform-launcher")

    //spring test
    testImplementation("org.springframework.boot:spring-boot-starter-data-jdbc-test")
    testImplementation("org.springframework.boot:spring-boot-testcontainers")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
    testImplementation("org.testcontainers:testcontainers-postgresql")
}

tasks.withType<JavaCompile>{
    this.options.compilerArgs.add("--enable-preview")
}

tasks.withType<BootJar>{
    this.archiveClassifier.set("boot")
}

tasks.withType<Jar>{
    enabled = true;
    manifest{

    }
    archiveClassifier.set("")
}

tasks.withType<Test> {
    useJUnitPlatform()
    maxHeapSize = "4g"
    jvmArgs = listOf("--enable-preview","--add-opens=java.base/java.lang.invoke=ALL-UNNAMED")
}