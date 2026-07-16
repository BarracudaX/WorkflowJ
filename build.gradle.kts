plugins {
    id("java")
    id("io.freefair.lombok") version "9.5.0"
}

group = "com.barracuda"
version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {

    implementation("org.apache.fory:fory-core:1.3.0")
    implementation("org.mapstruct:mapstruct:1.6.3")
    implementation("org.slf4j:slf4j-api:2.0.18")

    testImplementation("org.awaitility:awaitility:4.3.0")
    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly ("org.junit.platform:junit-platform-launcher")
}

tasks.withType<JavaCompile>(){
    this.options.compilerArgs.add("--enable-preview")
}

tasks.withType<Test> {
    useJUnitPlatform()
    maxHeapSize = "4g"
    jvmArgs = listOf("--enable-preview","--add-opens=java.base/java.lang.invoke=ALL-UNNAMED")
}