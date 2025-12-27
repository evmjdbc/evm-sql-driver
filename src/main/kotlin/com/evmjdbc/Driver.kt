package com.evmjdbc

import com.evmjdbc.connection.ConnectionProperties
import com.evmjdbc.connection.EvmConnection
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.util.Properties
import java.util.logging.Logger

/**
 * JDBC Driver for EVM-compatible blockchain JSON-RPC endpoints.
 *
 * URL format: jdbc:evm:<network>?rpcUrl=<url>&key=value...
 * Examples:
 *   jdbc:evm:mainnet?rpcUrl=https://eth.llamarpc.com
 *   jdbc:evm:sepolia?rpcUrl=https://rpc.sepolia.org
 *   jdbc:evm:custom?rpcUrl=http://localhost:8545
 */
class Driver : java.sql.Driver {
    private val logger = LoggerFactory.getLogger(Driver::class.java)

    /**
     * Attempt to connect to the given URL.
     *
     * @param url The URL of the database (jdbc:evm:...)
     * @param info A list of arbitrary string tag/value pairs as connection arguments
     * @return A Connection object representing a connection to the URL
     * @throws SQLException if a database access error occurs or the url is null
     */
    override fun connect(url: String?, info: Properties?): Connection? {
        if (url == null || !acceptsURL(url)) {
            return null
        }

        logger.info("Connecting to EVM endpoint: {}", maskUrl(url))

        return try {
            val properties = ConnectionProperties.parse(url, info)
            EvmConnection(properties)
        } catch (e: IllegalArgumentException) {
            throw SQLException("Invalid connection URL or properties: ${e.message}", "08001", e)
        } catch (e: Exception) {
            throw SQLException("Failed to connect: ${e.message}", "08001", e)
        }
    }

    /**
     * Check if the driver accepts the given URL.
     *
     * @param url The URL of the database
     * @return True if the driver can connect to the given URL
     */
    override fun acceptsURL(url: String?): Boolean {
        return url?.startsWith(ConnectionProperties.URL_PREFIX) == true
    }

    /**
     * Get information about the possible properties for this driver.
     *
     * @param url The URL of the database
     * @param info A proposed list of tag/value pairs
     * @return An array of DriverPropertyInfo objects describing possible properties
     */
    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        if (url == null) {
            return emptyArray()
        }
        return ConnectionProperties.getPropertyInfo(url, info)
    }

    /**
     * Get the driver's major version number.
     */
    override fun getMajorVersion(): Int = MAJOR_VERSION

    /**
     * Get the driver's minor version number.
     */
    override fun getMinorVersion(): Int = MINOR_VERSION

    /**
     * Report whether this is a genuine JDBC Compliant driver.
     * A driver may only report true if it passes the JDBC compliance tests.
     */
    override fun jdbcCompliant(): Boolean = false // Not fully JDBC compliant (read-only subset)

    /**
     * Return the parent Logger of all the Loggers used by this driver.
     */
    override fun getParentLogger(): Logger {
        throw java.sql.SQLFeatureNotSupportedException("java.util.logging not used")
    }

    /**
     * Mask sensitive parts of URL for logging.
     */
    private fun maskUrl(url: String): String {
        return url.replace(Regex("(api[_-]?key|key|token|secret)=([^&]+)", RegexOption.IGNORE_CASE)) {
            "${it.groupValues[1]}=***"
        }
    }

    companion object {
        const val MAJOR_VERSION = 1
        const val MINOR_VERSION = 0
        const val DRIVER_NAME = "EVM SQL Driver"
        const val DRIVER_VERSION = "$MAJOR_VERSION.$MINOR_VERSION.0"

        // Singleton instance for registration
        private val INSTANCE = Driver()

        init {
            // Auto-registration when class is loaded
            try {
                DriverManager.registerDriver(INSTANCE)
            } catch (e: SQLException) {
                throw ExceptionInInitializerError(e)
            }
        }
    }
}
