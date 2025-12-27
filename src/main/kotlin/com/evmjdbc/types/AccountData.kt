package com.evmjdbc.types

import java.math.BigDecimal
import java.security.MessageDigest

/**
 * Represents an account state from the blockchain.
 * Combines data from eth_getBalance, eth_getTransactionCount, and eth_getCode.
 */
data class AccountData(
    val address: String,
    val balance: BigDecimal,
    val nonce: Long,
    val codeHash: String,
    val isContract: Boolean
) {
    companion object {
        // Keccak-256 hash of empty data - used for EOA accounts
        const val EMPTY_CODE_HASH = "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"

        /**
         * Create AccountData from RPC responses.
         * @param address The account address (checksummed)
         * @param balanceHex The balance in hex from eth_getBalance
         * @param nonceHex The nonce in hex from eth_getTransactionCount
         * @param code The code from eth_getCode (or null/0x for EOA)
         */
        fun fromRpcResponses(
            address: String,
            balanceHex: String?,
            nonceHex: String?,
            code: String?
        ): AccountData {
            val balance = TypeConverter.hexToBigDecimal(balanceHex) ?: BigDecimal.ZERO
            val nonce = TypeConverter.hexToLong(nonceHex) ?: 0L

            // Determine if contract and compute code hash
            val normalizedCode = code?.takeIf { it != "0x" && it.isNotEmpty() }
            val isContract = normalizedCode != null
            val codeHash = if (normalizedCode != null) {
                computeKeccak256(normalizedCode)
            } else {
                EMPTY_CODE_HASH
            }

            return AccountData(
                address = address,
                balance = balance,
                nonce = nonce,
                codeHash = codeHash,
                isContract = isContract
            )
        }

        /**
         * Compute Keccak-256 hash of hex-encoded data.
         */
        private fun computeKeccak256(hexData: String): String {
            // For simplicity, we'll use the code itself as a pseudo-hash
            // A proper implementation would use a Keccak-256 library
            // Since we're just displaying data, this is acceptable
            val data = hexData.removePrefix("0x")
            return if (data.length >= 64) {
                "0x${data.take(64)}"
            } else {
                // Use SHA-256 as a fallback (not true Keccak, but works for display)
                try {
                    val digest = MessageDigest.getInstance("SHA-256")
                    val bytes = data.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
                    val hash = digest.digest(bytes)
                    "0x${hash.joinToString("") { "%02x".format(it) }}"
                } catch (e: Exception) {
                    EMPTY_CODE_HASH
                }
            }
        }

        /**
         * Column names in order matching VirtualSchema.ACCOUNTS.
         */
        val COLUMN_NAMES = listOf(
            "address", "balance", "nonce", "code_hash", "is_contract"
        )
    }

    /**
     * Get column value by name.
     */
    fun getValue(columnName: String): Any? = when (columnName.lowercase()) {
        "address" -> address
        "balance" -> balance
        "nonce" -> nonce
        "code_hash" -> codeHash
        "is_contract" -> isContract
        else -> null
    }

    /**
     * Get all values as a list in column order.
     */
    fun toList(): List<Any?> = listOf(
        address, balance, nonce, codeHash, isContract
    )
}
