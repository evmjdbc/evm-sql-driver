package com.evmjdbc.types

import com.google.gson.JsonObject

/**
 * Represents an event log from the blockchain.
 * Maps eth_getLogs response entries to SQL-friendly types.
 */
data class LogData(
    val blockNumber: Long,
    val blockHash: String,
    val transactionHash: String,
    val transactionIndex: Int,
    val logIndex: Int,
    val address: String,
    val topic0: String?,
    val topic1: String?,
    val topic2: String?,
    val topic3: String?,
    val data: String,
    val removed: Boolean
) {
    companion object {
        /**
         * Parse a log entry from JSON-RPC response.
         */
        fun fromJson(json: JsonObject): LogData {
            val topics = json.get("topics")?.asJsonArray

            fun getTopic(index: Int): String? {
                return if (topics != null && index < topics.size()) {
                    topics.get(index)?.asString
                } else null
            }

            return LogData(
                blockNumber = TypeConverter.hexToLong(json.get("blockNumber")?.asString) ?: 0L,
                blockHash = json.get("blockHash")?.asString ?: "",
                transactionHash = json.get("transactionHash")?.asString ?: "",
                transactionIndex = TypeConverter.hexToLong(json.get("transactionIndex")?.asString)?.toInt() ?: 0,
                logIndex = TypeConverter.hexToLong(json.get("logIndex")?.asString)?.toInt() ?: 0,
                address = TypeConverter.toChecksumAddress(json.get("address")?.asString) ?: "",
                topic0 = getTopic(0),
                topic1 = getTopic(1),
                topic2 = getTopic(2),
                topic3 = getTopic(3),
                data = json.get("data")?.asString ?: "0x",
                removed = json.get("removed")?.asBoolean ?: false
            )
        }

        /**
         * Column names in order matching VirtualSchema.LOGS.
         */
        val COLUMN_NAMES = listOf(
            "block_number", "block_hash", "transaction_hash", "transaction_index",
            "log_index", "address", "topic0", "topic1", "topic2", "topic3",
            "data", "removed"
        )
    }

    /**
     * Get column value by name.
     */
    fun getValue(columnName: String): Any? = when (columnName.lowercase()) {
        "block_number" -> blockNumber
        "block_hash" -> blockHash
        "transaction_hash" -> transactionHash
        "transaction_index" -> transactionIndex
        "log_index" -> logIndex
        "address" -> address
        "topic0" -> topic0
        "topic1" -> topic1
        "topic2" -> topic2
        "topic3" -> topic3
        "data" -> data
        "removed" -> removed
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        blockNumber, blockHash, transactionHash, transactionIndex,
        logIndex, address, topic0, topic1, topic2, topic3,
        data, removed
    )
}
