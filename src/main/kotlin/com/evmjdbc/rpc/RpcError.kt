package com.evmjdbc.rpc

import com.google.gson.JsonObject
import java.sql.SQLException
import java.sql.SQLNonTransientConnectionException
import java.sql.SQLSyntaxErrorException
import java.sql.SQLTransientConnectionException

/**
 * JSON-RPC error response from Ethereum RPC calls.
 */
data class RpcError(
    val code: Int,
    val message: String,
    val data: String? = null
) {
    companion object {
        // Standard JSON-RPC error codes
        const val PARSE_ERROR = -32700
        const val INVALID_REQUEST = -32600
        const val METHOD_NOT_FOUND = -32601
        const val INVALID_PARAMS = -32602
        const val INTERNAL_ERROR = -32603

        // Ethereum-specific error codes
        const val EXECUTION_ERROR = -32000
        const val RESOURCE_NOT_FOUND = -32001
        const val RESOURCE_UNAVAILABLE = -32002
        const val TRANSACTION_REJECTED = -32003
        const val METHOD_NOT_SUPPORTED = -32004
        const val LIMIT_EXCEEDED = -32005

        /**
         * Parse RpcError from JsonObject.
         */
        fun fromJson(obj: JsonObject): RpcError = RpcError(
            code = obj.get("code")?.asInt ?: INTERNAL_ERROR,
            message = obj.get("message")?.asString ?: "Unknown error",
            data = obj.get("data")?.asString
        )
    }

    /**
     * Convert to an appropriate SQLException subclass.
     */
    fun toSqlException(): SQLException {
        val fullMessage = buildString {
            append("[EVM-")
            append(kotlin.math.abs(code))
            append("] ")
            append(message)
            data?.let { append(". Data: $it") }
        }

        return when (code) {
            PARSE_ERROR, INVALID_REQUEST, INVALID_PARAMS ->
                SQLSyntaxErrorException(fullMessage)

            METHOD_NOT_FOUND, METHOD_NOT_SUPPORTED ->
                java.sql.SQLFeatureNotSupportedException(fullMessage)

            RESOURCE_NOT_FOUND ->
                SQLException(fullMessage, "02000") // NO_DATA

            RESOURCE_UNAVAILABLE ->
                SQLTransientConnectionException(fullMessage)

            LIMIT_EXCEEDED ->
                SQLTransientConnectionException(fullMessage)

            EXECUTION_ERROR ->
                SQLException(fullMessage)

            else ->
                if (code <= -32000 && code >= -32099) {
                    // Server error range
                    SQLNonTransientConnectionException(fullMessage)
                } else {
                    SQLException(fullMessage)
                }
        }
    }

    /**
     * Check if this error indicates a rate limit.
     */
    fun isRateLimit(): Boolean = code == LIMIT_EXCEEDED ||
            message.contains("rate limit", ignoreCase = true) ||
            message.contains("too many requests", ignoreCase = true)

    /**
     * Check if this error is transient and can be retried.
     */
    fun isRetryable(): Boolean = code in listOf(
        RESOURCE_UNAVAILABLE,
        LIMIT_EXCEEDED,
        INTERNAL_ERROR
    ) || isRateLimit()
}
