package com.evmjdbc.types

import com.google.gson.JsonObject
import java.math.BigDecimal
import java.sql.Timestamp
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Represents a block from the blockchain.
 * Maps eth_getBlockByNumber response to SQL-friendly types.
 */
data class BlockData(
    val number: Long,
    val hash: String,
    val parentHash: String,
    val nonce: String?,
    val sha3Uncles: String,
    val logsBloom: String,
    val transactionsRoot: String,
    val stateRoot: String,
    val receiptsRoot: String,
    val miner: String,
    val difficulty: BigDecimal,
    val totalDifficulty: BigDecimal?,
    val extraData: String,
    val size: Long,
    val gasLimit: Long,
    val gasUsed: Long,
    val timestamp: OffsetDateTime,
    val transactionCount: Int,
    val baseFeePerGas: BigDecimal?,
    val withdrawalsRoot: String?
) {
    companion object {
        /**
         * Parse a block from JSON-RPC response.
         */
        fun fromJson(json: JsonObject): BlockData {
            return BlockData(
                number = TypeConverter.hexToLong(json.get("number")?.asString) ?: 0L,
                hash = json.get("hash")?.asString ?: "",
                parentHash = json.get("parentHash")?.asString ?: "",
                nonce = json.get("nonce")?.asString,
                sha3Uncles = json.get("sha3Uncles")?.asString ?: "",
                logsBloom = json.get("logsBloom")?.asString ?: "",
                transactionsRoot = json.get("transactionsRoot")?.asString ?: "",
                stateRoot = json.get("stateRoot")?.asString ?: "",
                receiptsRoot = json.get("receiptsRoot")?.asString ?: "",
                miner = TypeConverter.toChecksumAddress(json.get("miner")?.asString) ?: "",
                difficulty = TypeConverter.hexToBigDecimal(json.get("difficulty")?.asString) ?: BigDecimal.ZERO,
                totalDifficulty = TypeConverter.hexToBigDecimal(json.get("totalDifficulty")?.asString),
                extraData = json.get("extraData")?.asString ?: "0x",
                size = TypeConverter.hexToLong(json.get("size")?.asString) ?: 0L,
                gasLimit = TypeConverter.hexToLong(json.get("gasLimit")?.asString) ?: 0L,
                gasUsed = TypeConverter.hexToLong(json.get("gasUsed")?.asString) ?: 0L,
                timestamp = TypeConverter.hexTimestampToOffsetDateTime(json.get("timestamp")?.asString)
                    ?: OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC),
                transactionCount = json.get("transactions")?.asJsonArray?.size() ?: 0,
                baseFeePerGas = TypeConverter.hexToBigDecimal(json.get("baseFeePerGas")?.asString),
                withdrawalsRoot = json.get("withdrawalsRoot")?.asString
            )
        }

        /**
         * Column names in order matching VirtualSchema.BLOCKS.
         */
        val COLUMN_NAMES = listOf(
            "number", "hash", "parent_hash", "nonce", "sha3_uncles", "logs_bloom",
            "transactions_root", "state_root", "receipts_root", "miner", "difficulty",
            "total_difficulty", "extra_data", "size", "gas_limit", "gas_used",
            "timestamp", "transaction_count", "base_fee_per_gas", "withdrawals_root"
        )
    }

    /**
     * Get column value by name.
     */
    fun getValue(columnName: String): Any? = when (columnName.lowercase()) {
        "number" -> number
        "hash" -> hash
        "parent_hash" -> parentHash
        "nonce" -> nonce
        "sha3_uncles" -> sha3Uncles
        "logs_bloom" -> logsBloom
        "transactions_root" -> transactionsRoot
        "state_root" -> stateRoot
        "receipts_root" -> receiptsRoot
        "miner" -> miner
        "difficulty" -> difficulty
        "total_difficulty" -> totalDifficulty
        "extra_data" -> extraData
        "size" -> size
        "gas_limit" -> gasLimit
        "gas_used" -> gasUsed
        "timestamp" -> timestamp
        "transaction_count" -> transactionCount
        "base_fee_per_gas" -> baseFeePerGas
        "withdrawals_root" -> withdrawalsRoot
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        number, hash, parentHash, nonce, sha3Uncles, logsBloom,
        transactionsRoot, stateRoot, receiptsRoot, miner, difficulty,
        totalDifficulty, extraData, size, gasLimit, gasUsed,
        timestamp, transactionCount, baseFeePerGas, withdrawalsRoot
    )
}
