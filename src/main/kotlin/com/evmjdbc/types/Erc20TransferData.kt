package com.evmjdbc.types

import java.math.BigDecimal

/**
 * Represents a decoded ERC20 Transfer event.
 * Transfer(address indexed from, address indexed to, uint256 value)
 */
data class Erc20TransferData(
    val blockNumber: Long,
    val logIndex: Int,
    val transactionHash: String,
    val tokenAddress: String,
    val fromAddress: String,
    val toAddress: String,
    val amount: BigDecimal
) {
    companion object {
        // keccak256("Transfer(address,address,uint256)")
        const val TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        /**
         * Try to parse a log as an ERC20 Transfer event.
         * Returns null if the log doesn't match the Transfer signature or is ERC721.
         */
        fun fromLog(log: LogData): Erc20TransferData? {
            // Must have Transfer topic0
            if (log.topic0?.lowercase() != TRANSFER_TOPIC.lowercase()) {
                return null
            }

            // ERC20 has 3 topics (topic0, from, to) and amount in data
            // ERC721 has 4 topics (topic0, from, to, tokenId)
            // If topic3 is present, it's likely ERC721
            if (log.topic3 != null) {
                return null
            }

            // Must have from and to in topics
            val from = log.topic1 ?: return null
            val to = log.topic2 ?: return null

            // Decode amount from data (uint256)
            val amount = decodeUint256(log.data) ?: return null

            return Erc20TransferData(
                blockNumber = log.blockNumber,
                logIndex = log.logIndex,
                transactionHash = log.transactionHash,
                tokenAddress = TypeConverter.toChecksumAddress(log.address) ?: log.address,
                fromAddress = topicToAddress(from),
                toAddress = topicToAddress(to),
                amount = amount
            )
        }

        /**
         * Convert a 32-byte topic to an address (last 20 bytes).
         */
        private fun topicToAddress(topic: String): String {
            val hex = topic.removePrefix("0x")
            // Address is the last 40 characters (20 bytes)
            val addressHex = if (hex.length >= 40) hex.takeLast(40) else hex
            return TypeConverter.toChecksumAddress("0x$addressHex") ?: "0x$addressHex"
        }

        /**
         * Decode a uint256 from hex data.
         */
        private fun decodeUint256(data: String): BigDecimal? {
            val hex = data.removePrefix("0x")
            if (hex.isEmpty() || hex == "0") return BigDecimal.ZERO
            return try {
                BigDecimal(java.math.BigInteger(hex, 16))
            } catch (e: Exception) {
                null
            }
        }

        /**
         * Column names in order matching VirtualSchema.ERC20_TRANSFERS.
         */
        val COLUMN_NAMES = listOf(
            "block_number", "log_index", "transaction_hash",
            "token_address", "from_address", "to_address", "amount"
        )
    }

    /**
     * Get column value by name.
     */
    fun getValue(columnName: String): Any? = when (columnName.lowercase()) {
        "block_number" -> blockNumber
        "log_index" -> logIndex
        "transaction_hash" -> transactionHash
        "token_address" -> tokenAddress
        "from_address" -> fromAddress
        "to_address" -> toAddress
        "amount" -> amount
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        blockNumber, logIndex, transactionHash,
        tokenAddress, fromAddress, toAddress, amount
    )
}
