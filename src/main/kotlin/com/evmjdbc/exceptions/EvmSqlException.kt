package com.evmjdbc.exceptions

import java.sql.SQLException

/**
 * Base exception for EVM SQL Driver errors.
 * Provides structured error codes and messages.
 */
open class EvmSqlException : SQLException {
    val errorCode: ErrorCode

    constructor(errorCode: ErrorCode, message: String? = null, cause: Throwable? = null) :
            super(formatMessage(errorCode, message), cause) {
        this.errorCode = errorCode
    }

    constructor(errorCode: ErrorCode, message: String?) :
            super(formatMessage(errorCode, message)) {
        this.errorCode = errorCode
    }

    companion object {
        private fun formatMessage(errorCode: ErrorCode, additionalMessage: String?): String {
            return buildString {
                append("[EVM-")
                append(errorCode.code)
                append("] ")
                append(errorCode.defaultMessage)
                additionalMessage?.let {
                    append(": ")
                    append(it)
                }
            }
        }
    }
}

/**
 * Error codes for EVM SQL Driver.
 */
enum class ErrorCode(val code: Int, val defaultMessage: String) {
    // Connection errors (1xxx)
    CONNECTION_FAILED(1001, "Failed to connect to RPC endpoint"),
    INVALID_URL(1002, "Invalid connection URL format"),
    AUTHENTICATION_FAILED(1003, "RPC authentication failed"),
    CONNECTION_CLOSED(1004, "Connection is closed"),

    // Query errors (2xxx)
    UNSUPPORTED_OPERATION(2001, "Operation not supported"),
    READ_ONLY_VIOLATION(2002, "EVM SQL Driver is read-only. Use eth_sendRawTransaction via RPC directly for write operations"),
    BLOCK_RANGE_EXCEEDED(2003, "Block range exceeds maximum"),
    INVALID_QUERY(2004, "Invalid SQL query"),
    UNSUPPORTED_SYNTAX(2005, "Unsupported SQL syntax"),
    TRANSLATION_ERROR(2006, "SQL to RPC translation failed"),

    // RPC errors (3xxx)
    RPC_ERROR(3001, "RPC call failed"),
    RATE_LIMITED(3002, "Rate limit exceeded"),
    RPC_TIMEOUT(3003, "RPC request timed out"),
    MALFORMED_RESPONSE(3004, "Malformed RPC response"),

    // Data errors (4xxx)
    TYPE_CONVERSION_ERROR(4001, "Type conversion failed"),
    INVALID_ADDRESS(4002, "Invalid Ethereum address"),
    INVALID_HASH(4003, "Invalid hash format"),
    OVERFLOW(4004, "Numeric overflow"),

    // Internal errors (5xxx)
    INTERNAL_ERROR(5001, "Internal driver error"),
    NOT_IMPLEMENTED(5002, "Feature not yet implemented")
}
