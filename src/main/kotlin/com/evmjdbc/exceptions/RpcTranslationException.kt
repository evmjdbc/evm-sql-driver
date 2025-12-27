package com.evmjdbc.exceptions

import java.sql.SQLSyntaxErrorException

/**
 * Exception thrown when SQL cannot be translated to RPC calls.
 */
class RpcTranslationException : SQLSyntaxErrorException {
    val tableName: String?
    val clause: String?

    constructor(message: String, tableName: String? = null, clause: String? = null) : super(
        buildMessage(message, tableName, clause)
    ) {
        this.tableName = tableName
        this.clause = clause
    }

    constructor(message: String, tableName: String? = null, clause: String? = null, cause: Throwable?) : super(
        buildMessage(message, tableName, clause),
        cause
    ) {
        this.tableName = tableName
        this.clause = clause
    }

    companion object {
        private fun buildMessage(message: String, tableName: String?, clause: String?): String {
            return buildString {
                append("[EVM-2004] ")
                append(message)
                tableName?.let { append(" (table: $it)") }
                clause?.let { append(" [clause: $it]") }
            }
        }

        /**
         * Create exception for unsupported table.
         */
        fun unsupportedTable(tableName: String): RpcTranslationException =
            RpcTranslationException("Unknown table '$tableName'. Valid tables: blocks, transactions, logs, accounts, erc20_transfers, chain_info", tableName)

        /**
         * Create exception for missing required filter.
         */
        fun missingRequiredFilter(tableName: String, filterName: String): RpcTranslationException =
            RpcTranslationException("Query on '$tableName' requires a $filterName filter", tableName, filterName)

        /**
         * Create exception for unsupported operator.
         */
        fun unsupportedOperator(operator: String, clause: String): RpcTranslationException =
            RpcTranslationException("Unsupported operator '$operator'", clause = clause)

        /**
         * Create exception for block range exceeded.
         */
        fun blockRangeExceeded(requested: Long, maximum: Long): RpcTranslationException =
            RpcTranslationException(
                "Block range exceeds maximum ($maximum). Use LIMIT or narrow your WHERE clause. Requested range: $requested blocks"
            )
    }
}
