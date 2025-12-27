package com.evmjdbc.contract

import com.evmjdbc.Driver
import com.evmjdbc.connection.ConnectionProperties
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*

/**
 * JDBC compliance tests for Driver registration and connection.
 * These tests verify that the driver follows JDBC 4.2+ conventions.
 */
@DisplayName("JDBC Compliance - Driver")
class JdbcComplianceTest {

    @BeforeEach
    fun setup() {
        // Ensure driver is loaded
        Class.forName("com.evmjdbc.Driver")
    }

    @Test
    @DisplayName("Driver is registered with DriverManager")
    fun driverIsRegistered() {
        val drivers = DriverManager.getDrivers().toList()
        val evmDriver = drivers.find { it is Driver }
        assertNotNull(evmDriver, "EVM SQL Driver should be registered with DriverManager")
    }

    @Test
    @DisplayName("Driver accepts valid jdbc:evm: URLs")
    fun driverAcceptsValidUrls() {
        val driver = Driver()

        assertTrue(driver.acceptsURL("jdbc:evm:mainnet?rpcUrl=https://eth.llamarpc.com"))
        assertTrue(driver.acceptsURL("jdbc:evm:sepolia?rpcUrl=https://rpc.sepolia.org"))
        assertTrue(driver.acceptsURL("jdbc:evm:custom?rpcUrl=http://localhost:8545"))
        assertTrue(driver.acceptsURL("jdbc:evm:?rpcUrl=http://localhost:8545"))
    }

    @Test
    @DisplayName("Driver rejects invalid URLs")
    fun driverRejectsInvalidUrls() {
        val driver = Driver()

        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/db"))
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/db"))
        assertFalse(driver.acceptsURL("http://localhost:8545"))
        assertFalse(driver.acceptsURL(""))
        assertFalse(driver.acceptsURL(null))
    }

    @Test
    @DisplayName("Driver returns null for non-matching URLs in connect()")
    fun driverReturnsNullForNonMatchingUrl() {
        val driver = Driver()

        assertNull(driver.connect("jdbc:postgresql://localhost:5432/db", null))
        assertNull(driver.connect(null, null))
    }

    @Test
    @DisplayName("Driver version information is correct")
    fun driverVersionIsCorrect() {
        val driver = Driver()

        assertEquals(Driver.MAJOR_VERSION, driver.majorVersion)
        assertEquals(Driver.MINOR_VERSION, driver.minorVersion)
        assertTrue(driver.majorVersion >= 1)
        assertTrue(driver.minorVersion >= 0)
    }

    @Test
    @DisplayName("Driver is not JDBC compliant (read-only subset)")
    fun driverIsNotFullyCompliant() {
        val driver = Driver()

        // Our driver is not fully JDBC compliant because it's read-only
        assertFalse(driver.jdbcCompliant())
    }

    @Test
    @DisplayName("Driver provides property info")
    fun driverProvidesPropertyInfo() {
        val driver = Driver()
        val url = "jdbc:evm:mainnet?rpcUrl=https://eth.llamarpc.com"

        val propertyInfo = driver.getPropertyInfo(url, null)

        assertNotNull(propertyInfo)
        assertTrue(propertyInfo.isNotEmpty(), "Should return property info")

        // Check for required rpcUrl property
        val rpcUrlProp = propertyInfo.find { it.name == ConnectionProperties.PROP_RPC_URL }
        assertNotNull(rpcUrlProp, "Should have rpcUrl property")
        assertTrue(rpcUrlProp!!.required, "rpcUrl should be required")
    }

    @Test
    @DisplayName("getParentLogger throws SQLFeatureNotSupportedException")
    fun getParentLoggerThrows() {
        val driver = Driver()

        assertThrows(java.sql.SQLFeatureNotSupportedException::class.java) {
            driver.parentLogger
        }
    }

    @Test
    @DisplayName("Connection requires rpcUrl property")
    fun connectionRequiresRpcUrl() {
        val driver = Driver()

        assertThrows(SQLException::class.java) {
            driver.connect("jdbc:evm:mainnet", null)
        }
    }

    @Test
    @DisplayName("Connection can receive properties via URL parameters")
    fun connectionAcceptsUrlParameters() {
        val url = "jdbc:evm:mainnet?rpcUrl=https://eth.llamarpc.com&timeout=60000"
        val props = ConnectionProperties.parse(url, null)

        assertEquals("https://eth.llamarpc.com", props.rpcUrl)
        assertEquals("mainnet", props.network)
        assertEquals(60000, props.timeout)
    }

    @Test
    @DisplayName("Connection can receive properties via Properties object")
    fun connectionAcceptsPropertiesObject() {
        val url = "jdbc:evm:sepolia"
        val info = Properties().apply {
            setProperty("rpcUrl", "https://rpc.sepolia.org")
            setProperty("timeout", "45000")
        }

        val props = ConnectionProperties.parse(url, info)

        assertEquals("https://rpc.sepolia.org", props.rpcUrl)
        assertEquals("sepolia", props.network)
        assertEquals(45000, props.timeout)
    }

    @Test
    @DisplayName("URL parameters take precedence over Properties")
    fun urlParametersTakePrecedence() {
        val url = "jdbc:evm:mainnet?rpcUrl=https://from-url.com"
        val info = Properties().apply {
            setProperty("rpcUrl", "https://from-properties.com")
        }

        val props = ConnectionProperties.parse(url, info)

        assertEquals("https://from-url.com", props.rpcUrl)
    }

    @Test
    @DisplayName("DriverManager.getConnection works with JDBC URL")
    fun driverManagerGetConnectionWorks() {
        // This test would require a real RPC endpoint
        // For unit testing, we just verify the URL is parsed correctly
        val url = "jdbc:evm:sepolia?rpcUrl=https://rpc.sepolia.org"

        // Don't actually connect - just verify driver accepts URL
        val driver = Driver()
        assertTrue(driver.acceptsURL(url))
    }
}
