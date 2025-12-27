package com.evmjdbc.connection

import com.evmjdbc.types.InputDataMode
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.sql.DriverPropertyInfo
import java.util.Properties

/**
 * Connection properties for EVM SQL Driver.
 * Parses URL parameters and Properties object.
 */
data class ConnectionProperties(
    val rpcUrl: String,
    val network: String = "custom",
    val apiKey: String? = null,
    val rpcHeaders: Map<String, String> = emptyMap(),
    val timeout: Int = DEFAULT_TIMEOUT,
    val readTimeout: Int = DEFAULT_READ_TIMEOUT,
    val maxRetries: Int = DEFAULT_MAX_RETRIES,
    val maxBlockRange: Int = DEFAULT_MAX_BLOCK_RANGE,
    val cacheEnabled: Boolean = DEFAULT_CACHE_ENABLED,
    val cacheTtlSeconds: Int = DEFAULT_CACHE_TTL,
    val inputDataMode: InputDataMode = InputDataMode.TRUNCATED,
    val nullForZero: Boolean = DEFAULT_NULL_FOR_ZERO,
    val numericAsString: Boolean = DEFAULT_NUMERIC_AS_STRING,
    val maxRowsForAggregation: Int = DEFAULT_MAX_ROWS_AGGREGATION
) {
    companion object {
        // Property names
        const val PROP_RPC_URL = "rpcUrl"
        const val PROP_API_KEY = "apiKey"
        const val PROP_RPC_HEADERS = "rpcHeaders"
        const val PROP_TIMEOUT = "timeout"
        const val PROP_READ_TIMEOUT = "readTimeout"
        const val PROP_MAX_RETRIES = "maxRetries"
        const val PROP_MAX_BLOCK_RANGE = "maxBlockRange"
        const val PROP_CACHE_ENABLED = "cacheEnabled"
        const val PROP_CACHE_TTL = "cacheTtlSeconds"
        const val PROP_INPUT_DATA_MODE = "inputDataMode"
        const val PROP_NULL_FOR_ZERO = "nullForZero"
        const val PROP_NUMERIC_AS_STRING = "numericAsString"
        const val PROP_MAX_ROWS_AGGREGATION = "maxRowsForAggregation"

        // Defaults
        const val DEFAULT_TIMEOUT = 30000
        const val DEFAULT_READ_TIMEOUT = 60000
        const val DEFAULT_MAX_RETRIES = 5
        const val DEFAULT_MAX_BLOCK_RANGE = 1000
        const val DEFAULT_CACHE_ENABLED = true
        const val DEFAULT_CACHE_TTL = 300
        const val DEFAULT_NULL_FOR_ZERO = false
        const val DEFAULT_NUMERIC_AS_STRING = false
        const val DEFAULT_MAX_ROWS_AGGREGATION = 100000

        // URL prefix
        const val URL_PREFIX = "jdbc:evm:"

        /**
         * Parse connection URL and optional Properties.
         * URL format: jdbc:evm:<network>?rpcUrl=<url>&key=value...
         */
        fun parse(url: String, info: Properties?): ConnectionProperties {
            require(url.startsWith(URL_PREFIX)) { "Invalid URL format. Expected: jdbc:evm:<network>?rpcUrl=<url>" }

            val withoutPrefix = url.removePrefix(URL_PREFIX)
            val parts = withoutPrefix.split("?", limit = 2)
            val network = parts[0].ifEmpty { "custom" }
            val queryString = parts.getOrNull(1) ?: ""

            // Parse URL parameters
            val urlParams = parseQueryString(queryString)

            // Merge with Properties (URL takes precedence)
            val merged = mutableMapOf<String, String>()
            info?.forEach { key, value -> merged[key.toString()] = value.toString() }
            merged.putAll(urlParams)

            val rpcUrl = merged[PROP_RPC_URL]
                ?: throw IllegalArgumentException("Missing required property: $PROP_RPC_URL")

            return ConnectionProperties(
                rpcUrl = rpcUrl,
                network = network,
                apiKey = merged[PROP_API_KEY],
                rpcHeaders = parseHeaders(merged[PROP_RPC_HEADERS]),
                timeout = merged[PROP_TIMEOUT]?.toIntOrNull() ?: DEFAULT_TIMEOUT,
                readTimeout = merged[PROP_READ_TIMEOUT]?.toIntOrNull() ?: DEFAULT_READ_TIMEOUT,
                maxRetries = merged[PROP_MAX_RETRIES]?.toIntOrNull() ?: DEFAULT_MAX_RETRIES,
                maxBlockRange = merged[PROP_MAX_BLOCK_RANGE]?.toIntOrNull() ?: DEFAULT_MAX_BLOCK_RANGE,
                cacheEnabled = merged[PROP_CACHE_ENABLED]?.toBooleanStrictOrNull() ?: DEFAULT_CACHE_ENABLED,
                cacheTtlSeconds = merged[PROP_CACHE_TTL]?.toIntOrNull() ?: DEFAULT_CACHE_TTL,
                inputDataMode = parseInputDataMode(merged[PROP_INPUT_DATA_MODE]),
                nullForZero = merged[PROP_NULL_FOR_ZERO]?.toBooleanStrictOrNull() ?: DEFAULT_NULL_FOR_ZERO,
                numericAsString = merged[PROP_NUMERIC_AS_STRING]?.toBooleanStrictOrNull() ?: DEFAULT_NUMERIC_AS_STRING,
                maxRowsForAggregation = merged[PROP_MAX_ROWS_AGGREGATION]?.toIntOrNull() ?: DEFAULT_MAX_ROWS_AGGREGATION
            )
        }

        private fun parseQueryString(queryString: String): Map<String, String> {
            if (queryString.isBlank()) return emptyMap()
            return queryString.split("&")
                .filter { it.contains("=") }
                .associate { param ->
                    val (key, value) = param.split("=", limit = 2)
                    URLDecoder.decode(key, StandardCharsets.UTF_8) to
                            URLDecoder.decode(value, StandardCharsets.UTF_8)
                }
        }

        private fun parseHeaders(headerString: String?): Map<String, String> {
            if (headerString.isNullOrBlank()) return emptyMap()
            return headerString.split(";")
                .filter { it.contains(":") }
                .associate { header ->
                    val (key, value) = header.split(":", limit = 2)
                    key.trim() to value.trim()
                }
        }

        private fun parseInputDataMode(mode: String?): InputDataMode {
            return when (mode?.lowercase()) {
                "full" -> InputDataMode.FULL
                "hash" -> InputDataMode.HASH
                "truncated" -> InputDataMode.TRUNCATED
                else -> InputDataMode.TRUNCATED
            }
        }

        /**
         * Get driver property info for DataGrip.
         */
        fun getPropertyInfo(url: String, info: Properties?): Array<DriverPropertyInfo> {
            val props = try {
                parse(url, info)
            } catch (e: Exception) {
                null
            }

            return arrayOf(
                DriverPropertyInfo(PROP_RPC_URL, props?.rpcUrl).apply {
                    required = true
                    description = "JSON-RPC endpoint URL"
                },
                DriverPropertyInfo(PROP_API_KEY, props?.apiKey).apply {
                    required = false
                    description = "API key (added to URL or headers)"
                },
                DriverPropertyInfo(PROP_RPC_HEADERS, null).apply {
                    required = false
                    description = "Custom headers (key:value;key2:value2)"
                },
                DriverPropertyInfo(PROP_TIMEOUT, props?.timeout?.toString() ?: DEFAULT_TIMEOUT.toString()).apply {
                    required = false
                    description = "Connection timeout (ms)"
                },
                DriverPropertyInfo(PROP_READ_TIMEOUT, props?.readTimeout?.toString() ?: DEFAULT_READ_TIMEOUT.toString()).apply {
                    required = false
                    description = "Read timeout (ms)"
                },
                DriverPropertyInfo(PROP_MAX_RETRIES, props?.maxRetries?.toString() ?: DEFAULT_MAX_RETRIES.toString()).apply {
                    required = false
                    description = "Max retry attempts"
                },
                DriverPropertyInfo(PROP_MAX_BLOCK_RANGE, props?.maxBlockRange?.toString() ?: DEFAULT_MAX_BLOCK_RANGE.toString()).apply {
                    required = false
                    description = "Max blocks per query"
                },
                DriverPropertyInfo(PROP_CACHE_ENABLED, props?.cacheEnabled?.toString() ?: DEFAULT_CACHE_ENABLED.toString()).apply {
                    required = false
                    description = "Enable response caching"
                    choices = arrayOf("true", "false")
                },
                DriverPropertyInfo(PROP_INPUT_DATA_MODE, props?.inputDataMode?.name?.lowercase() ?: "truncated").apply {
                    required = false
                    description = "Transaction input data mode"
                    choices = arrayOf("full", "hash", "truncated")
                },
                DriverPropertyInfo(PROP_NULL_FOR_ZERO, props?.nullForZero?.toString() ?: DEFAULT_NULL_FOR_ZERO.toString()).apply {
                    required = false
                    description = "Return NULL instead of 0"
                    choices = arrayOf("true", "false")
                }
            )
        }
    }

    /**
     * Mask sensitive parts for logging/display.
     */
    fun toSafeString(): String {
        val maskedUrl = rpcUrl.replace(Regex("(api[_-]?key|key|token|secret)=([^&]+)", RegexOption.IGNORE_CASE)) {
            "${it.groupValues[1]}=***"
        }
        return "ConnectionProperties(rpcUrl=$maskedUrl, network=$network, timeout=$timeout, maxBlockRange=$maxBlockRange)"
    }
}
