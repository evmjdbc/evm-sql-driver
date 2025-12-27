package com.evmjdbc.types

import java.math.BigDecimal

/**
 * Represents a decoded ERC721 Transfer event.
 * Transfer(address indexed from, address indexed to, uint256 indexed tokenId)
 */
data class Erc721TransferData(
    val blockNumber: Long,
    val logIndex: Int,
    val transactionHash: String,
    val tokenAddress: String,
    val fromAddress: String,
    val toAddress: String,
    val tokenId: BigDecimal
) {
    companion object {
        // keccak256("Transfer(address,address,uint256)") - same as ERC20
        const val TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        /**
         * Try to parse a log as an ERC721 Transfer event.
         * Returns null if the log doesn't match.
         */
        fun fromLog(log: LogData): Erc721TransferData? {
            // Must have Transfer topic0
            if (log.topic0?.lowercase() != TRANSFER_TOPIC.lowercase()) {
                return null
            }

            // ERC721 has 4 topics (topic0, from, to, tokenId)
            // topic3 contains the tokenId
            val from = log.topic1 ?: return null
            val to = log.topic2 ?: return null
            val tokenIdTopic = log.topic3 ?: return null

            // Decode tokenId from topic3
            val tokenId = decodeUint256(tokenIdTopic) ?: return null

            return Erc721TransferData(
                blockNumber = log.blockNumber,
                logIndex = log.logIndex,
                transactionHash = log.transactionHash,
                tokenAddress = TypeConverter.toChecksumAddress(log.address) ?: log.address,
                fromAddress = topicToAddress(from),
                toAddress = topicToAddress(to),
                tokenId = tokenId
            )
        }

        /**
         * Convert a 32-byte topic to an address (last 20 bytes).
         */
        private fun topicToAddress(topic: String): String {
            val hex = topic.removePrefix("0x")
            val addressHex = if (hex.length >= 40) hex.takeLast(40) else hex
            return TypeConverter.toChecksumAddress("0x$addressHex") ?: "0x$addressHex"
        }

        /**
         * Decode a uint256 from hex topic.
         */
        private fun decodeUint256(hex: String): BigDecimal? {
            val clean = hex.removePrefix("0x")
            if (clean.isEmpty()) return BigDecimal.ZERO
            return try {
                BigDecimal(java.math.BigInteger(clean, 16))
            } catch (e: Exception) {
                null
            }
        }

        /**
         * Column names in order matching VirtualSchema.ERC721_TRANSFERS.
         */
        val COLUMN_NAMES = listOf(
            "block_number", "log_index", "transaction_hash",
            "token_address", "from_address", "to_address", "token_id"
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
        "token_id" -> tokenId
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        blockNumber, logIndex, transactionHash,
        tokenAddress, fromAddress, toAddress, tokenId
    )
}
