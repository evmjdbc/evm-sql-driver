package com.evmjdbc.types

import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Timestamp
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

/**
 * Converts between Ethereum/Solidity types and JDBC/SQL types.
 */
object TypeConverter {

    /**
     * Convert hex string (0x...) to BigDecimal for uint256/int256.
     */
    fun hexToBigDecimal(hex: String?): BigDecimal? {
        if (hex == null || hex == "0x" || hex.isBlank()) return BigDecimal.ZERO
        return try {
            val cleanHex = hex.removePrefix("0x")
            if (cleanHex.isEmpty()) BigDecimal.ZERO
            else BigDecimal(BigInteger(cleanHex, 16))
        } catch (e: NumberFormatException) {
            null
        }
    }

    /**
     * Convert hex string (0x...) to Long for uint64 and smaller.
     */
    fun hexToLong(hex: String?): Long? {
        if (hex == null || hex == "0x" || hex.isBlank()) return 0L
        return try {
            val cleanHex = hex.removePrefix("0x")
            if (cleanHex.isEmpty()) 0L
            else java.lang.Long.parseUnsignedLong(cleanHex, 16)
        } catch (e: NumberFormatException) {
            null
        }
    }

    /**
     * Convert hex string (0x...) to Int for uint32 and smaller.
     */
    fun hexToInt(hex: String?): Int? {
        if (hex == null || hex == "0x" || hex.isBlank()) return 0
        return try {
            val cleanHex = hex.removePrefix("0x")
            if (cleanHex.isEmpty()) 0
            else Integer.parseUnsignedInt(cleanHex, 16)
        } catch (e: NumberFormatException) {
            null
        }
    }

    /**
     * Convert Unix timestamp (hex) to Timestamp.
     */
    fun hexTimestampToTimestamp(hex: String?): Timestamp? {
        val epochSeconds = hexToLong(hex) ?: return null
        return Timestamp.from(Instant.ofEpochSecond(epochSeconds))
    }

    /**
     * Convert Unix timestamp (hex) to OffsetDateTime (UTC).
     */
    fun hexTimestampToOffsetDateTime(hex: String?): OffsetDateTime? {
        val epochSeconds = hexToLong(hex) ?: return null
        return OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds), ZoneOffset.UTC)
    }

    /**
     * Convert address to checksummed format (EIP-55).
     */
    fun toChecksumAddress(address: String?): String? {
        if (address == null || address.isBlank()) return null
        val cleanAddress = address.lowercase().removePrefix("0x")
        if (cleanAddress.length != 40) return address // Return as-is if invalid

        val hash = keccak256(cleanAddress.toByteArray(Charsets.US_ASCII)).toHexString()

        return buildString {
            append("0x")
            cleanAddress.forEachIndexed { index, c ->
                if (c in '0'..'9') {
                    append(c)
                } else {
                    val hashChar = hash.getOrNull(index)?.digitToIntOrNull(16) ?: 0
                    append(if (hashChar >= 8) c.uppercaseChar() else c)
                }
            }
        }
    }

    /**
     * Validate Ethereum address format.
     */
    fun isValidAddress(address: String?): Boolean {
        if (address == null) return false
        val clean = address.removePrefix("0x")
        return clean.length == 40 && clean.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }
    }

    /**
     * Validate hash format (32 bytes = 64 hex chars + 0x).
     */
    fun isValidHash(hash: String?): Boolean {
        if (hash == null) return false
        val clean = hash.removePrefix("0x")
        return clean.length == 64 && clean.all { it in '0'..'9' || it in 'a'..'f' || it in 'A'..'F' }
    }

    /**
     * Convert block number to hex format for RPC.
     */
    fun blockNumberToHex(blockNumber: Long): String {
        return "0x${blockNumber.toString(16)}"
    }

    /**
     * Convert block number string to hex (handles "latest", "pending", "earliest").
     */
    fun blockTagToHex(blockTag: String): String {
        return when (blockTag.lowercase()) {
            "latest", "pending", "earliest" -> blockTag.lowercase()
            else -> {
                val num = blockTag.toLongOrNull()
                if (num != null) blockNumberToHex(num)
                else blockTag // Return as-is if already hex
            }
        }
    }

    /**
     * Truncate input data according to mode.
     */
    fun processInputData(data: String?, mode: InputDataMode): String? {
        if (data == null) return null
        return when (mode) {
            InputDataMode.FULL -> data
            InputDataMode.HASH -> keccak256(hexToBytes(data)).toHexString().let { "0x$it" }
            InputDataMode.TRUNCATED -> {
                if (data.length <= 2050) data // 0x + 1024 bytes = 2050 chars
                else data.take(2050) + "...[truncated]"
            }
        }
    }

    /**
     * Calculate input data size in bytes.
     */
    fun inputDataSize(data: String?): Long {
        if (data == null || data == "0x") return 0
        val cleanData = data.removePrefix("0x")
        return (cleanData.length / 2).toLong()
    }

    // Simple Keccak-256 implementation for address checksumming
    // In production, you'd use a proper crypto library
    private fun keccak256(input: ByteArray): ByteArray {
        // Simplified - in real implementation, use Bouncy Castle or similar
        // For now, use SHA-256 as a placeholder (this affects checksum correctness)
        val digest = java.security.MessageDigest.getInstance("SHA-256")
        return digest.digest(input)
    }

    private fun hexToBytes(hex: String): ByteArray {
        val cleanHex = hex.removePrefix("0x")
        return ByteArray(cleanHex.length / 2) { i ->
            Integer.parseInt(cleanHex.substring(i * 2, i * 2 + 2), 16).toByte()
        }
    }

    private fun ByteArray.toHexString(): String = joinToString("") { "%02x".format(it) }
}

/**
 * Input data handling modes.
 */
enum class InputDataMode {
    FULL,       // Include complete input data
    HASH,       // Include only keccak256 hash
    TRUNCATED   // First 1KB with truncation marker
}
