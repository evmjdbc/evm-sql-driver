package com.evmjdbc.types

import com.google.gson.JsonObject
import java.math.BigDecimal

/**
 * Represents a transaction from the blockchain.
 * Combines data from eth_getTransactionByHash and eth_getTransactionReceipt.
 */
data class TransactionData(
    val hash: String,
    val blockNumber: Long,
    val blockHash: String,
    val transactionIndex: Int,
    val fromAddress: String,
    val toAddress: String?,
    val value: BigDecimal,
    val gas: Long,
    val gasPrice: BigDecimal?,
    val maxFeePerGas: BigDecimal?,
    val maxPriorityFeePerGas: BigDecimal?,
    val input: String,
    val inputSize: Long,
    val nonce: Long,
    val type: Int,
    val chainId: Long?,
    val v: Long,
    val r: String,
    val s: String,
    // Receipt fields
    val status: Int,
    val gasUsed: Long,
    val effectiveGasPrice: BigDecimal,
    val contractAddress: String?,
    val logsCount: Int
) {
    companion object {
        /**
         * Parse a transaction from JSON-RPC response.
         * Can handle either a standalone transaction or one embedded in a block.
         */
        fun fromJson(json: JsonObject, receipt: JsonObject? = null): TransactionData {
            val input = json.get("input")?.asString ?: "0x"
            val inputBytes = if (input.startsWith("0x")) (input.length - 2) / 2 else input.length / 2

            return TransactionData(
                hash = json.get("hash")?.asString ?: "",
                blockNumber = TypeConverter.hexToLong(json.get("blockNumber")?.asString) ?: 0L,
                blockHash = json.get("blockHash")?.asString ?: "",
                transactionIndex = TypeConverter.hexToLong(json.get("transactionIndex")?.asString)?.toInt() ?: 0,
                fromAddress = TypeConverter.toChecksumAddress(json.get("from")?.asString) ?: "",
                toAddress = json.get("to")?.let { if (it.isJsonNull) null else TypeConverter.toChecksumAddress(it.asString) },
                value = TypeConverter.hexToBigDecimal(json.get("value")?.asString) ?: BigDecimal.ZERO,
                gas = TypeConverter.hexToLong(json.get("gas")?.asString) ?: 0L,
                gasPrice = TypeConverter.hexToBigDecimal(json.get("gasPrice")?.asString),
                maxFeePerGas = TypeConverter.hexToBigDecimal(json.get("maxFeePerGas")?.asString),
                maxPriorityFeePerGas = TypeConverter.hexToBigDecimal(json.get("maxPriorityFeePerGas")?.asString),
                input = input,
                inputSize = inputBytes.toLong(),
                nonce = TypeConverter.hexToLong(json.get("nonce")?.asString) ?: 0L,
                type = TypeConverter.hexToLong(json.get("type")?.asString)?.toInt() ?: 0,
                chainId = TypeConverter.hexToLong(json.get("chainId")?.asString),
                v = TypeConverter.hexToLong(json.get("v")?.asString) ?: 0L,
                r = json.get("r")?.asString ?: "",
                s = json.get("s")?.asString ?: "",
                // Receipt fields - use provided receipt or defaults
                status = receipt?.let { TypeConverter.hexToLong(it.get("status")?.asString)?.toInt() } ?: 1,
                gasUsed = receipt?.let { TypeConverter.hexToLong(it.get("gasUsed")?.asString) } ?: 0L,
                effectiveGasPrice = receipt?.let { TypeConverter.hexToBigDecimal(it.get("effectiveGasPrice")?.asString) }
                    ?: TypeConverter.hexToBigDecimal(json.get("gasPrice")?.asString)
                    ?: BigDecimal.ZERO,
                contractAddress = receipt?.get("contractAddress")?.let {
                    if (it.isJsonNull) null else TypeConverter.toChecksumAddress(it.asString)
                },
                logsCount = receipt?.get("logs")?.asJsonArray?.size() ?: 0
            )
        }

        /**
         * Column names in order matching VirtualSchema.TRANSACTIONS.
         */
        val COLUMN_NAMES = listOf(
            "hash", "block_number", "block_hash", "transaction_index",
            "from_address", "to_address", "value", "gas", "gas_price",
            "max_fee_per_gas", "max_priority_fee_per_gas", "input", "input_size",
            "nonce", "type", "chain_id", "v", "r", "s",
            "status", "gas_used", "effective_gas_price", "contract_address", "logs_count"
        )
    }

    /**
     * Get column value by name.
     */
    fun getValue(columnName: String): Any? = when (columnName.lowercase()) {
        "hash" -> hash
        "block_number" -> blockNumber
        "block_hash" -> blockHash
        "transaction_index" -> transactionIndex
        "from_address" -> fromAddress
        "to_address" -> toAddress
        "value" -> value
        "gas" -> gas
        "gas_price" -> gasPrice
        "max_fee_per_gas" -> maxFeePerGas
        "max_priority_fee_per_gas" -> maxPriorityFeePerGas
        "input" -> input
        "input_size" -> inputSize
        "nonce" -> nonce
        "type" -> type
        "chain_id" -> chainId
        "v" -> v
        "r" -> r
        "s" -> s
        "status" -> status
        "gas_used" -> gasUsed
        "effective_gas_price" -> effectiveGasPrice
        "contract_address" -> contractAddress
        "logs_count" -> logsCount
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        hash, blockNumber, blockHash, transactionIndex,
        fromAddress, toAddress, value, gas, gasPrice,
        maxFeePerGas, maxPriorityFeePerGas, input, inputSize,
        nonce, type, chainId, v, r, s,
        status, gasUsed, effectiveGasPrice, contractAddress, logsCount
    )
}
