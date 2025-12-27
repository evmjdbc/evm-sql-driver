pluginManagement {
    repositories {
        mavenCentral()
        gradlePluginPortal()
    }
}

plugins {
    id("com.gradleup.nmcp.settings") version "1.4.0"
}

rootProject.name = "evm-jdbc-driver"

nmcpSettings {
    centralPortal {
        username = providers.gradleProperty("sonatypeUsername")
        password = providers.gradleProperty("sonatypePassword")
        publishingType = "AUTOMATIC"
    }
}
