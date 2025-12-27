plugins {
    kotlin("jvm") version "2.1.20"
    id("com.gradleup.shadow") version "9.0.0-beta4"
    `maven-publish`
    signing
}

group = "com.evmjdbc"
version = "1.0.0"

repositories {
    mavenCentral()
}

// Target JVM 21 for DataGrip compatibility
tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
    }
}

tasks.withType<JavaCompile>().configureEach {
    sourceCompatibility = "21"
    targetCompatibility = "21"
}

dependencies {
    // SQL parsing
    implementation("com.github.jsqlparser:jsqlparser:4.7")

    // JSON parsing
    implementation("com.google.code.gson:gson:2.10.1")

    // Logging facade (no implementation bundled)
    implementation("org.slf4j:slf4j-api:2.0.9")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("io.mockk:mockk:1.13.8")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.9")
}

tasks.test {
    useJUnitPlatform()
}

tasks.shadowJar {
    archiveBaseName.set("evm-jdbc-driver")
    archiveClassifier.set("all")
    archiveVersion.set(project.version.toString())

    manifest {
        attributes(
            "Implementation-Title" to "EVM SQL Driver",
            "Implementation-Version" to project.version,
            "Main-Class" to "com.evmjdbc.Driver"
        )
    }

    // Relocate dependencies to avoid conflicts
    relocate("com.google.gson", "com.evmjdbc.shaded.gson")
    relocate("net.sf.jsqlparser", "com.evmjdbc.shaded.jsqlparser")
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to "EVM SQL Driver",
            "Implementation-Version" to project.version
        )
    }
}

// Create javadoc and sources jars (required for Maven Central)
val javadocJar by tasks.registering(Jar::class) {
    archiveClassifier.set("javadoc")
    from(tasks.named("javadoc"))
}

val sourcesJar by tasks.registering(Jar::class) {
    archiveClassifier.set("sources")
    from(sourceSets.main.get().allSource)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = project.group.toString()
            artifactId = "evm-jdbc-driver"
            version = project.version.toString()

            // Use the shadow jar as the main artifact
            artifact(tasks.shadowJar)
            artifact(javadocJar)
            artifact(sourcesJar)

            pom {
                name.set("EVM JDBC Driver")
                description.set("JDBC driver for querying EVM-compatible blockchains (Ethereum, Avalanche, Polygon, etc.) using SQL")
                url.set("https://github.com/evmjdbc/evm-sql-driver")

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("evmjdbc")
                        name.set("EVM JDBC")
                        url.set("https://evmjdbc.com")
                    }
                }

                scm {
                    url.set("https://github.com/evmjdbc/evm-sql-driver")
                    connection.set("scm:git:git://github.com/evmjdbc/evm-sql-driver.git")
                    developerConnection.set("scm:git:ssh://github.com/evmjdbc/evm-sql-driver.git")
                }
            }
        }
    }
}

signing {
    val signingKey = findProperty("signing.key") as String? ?: System.getenv("GPG_SIGNING_KEY")
    val signingPassword = findProperty("signing.password") as String? ?: System.getenv("GPG_SIGNING_PASSWORD")

    if (signingKey != null) {
        useInMemoryPgpKeys(signingKey, signingPassword ?: "")
    }

    sign(publishing.publications["mavenJava"])
}

