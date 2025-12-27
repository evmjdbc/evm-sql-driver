package com.evmjdbc.rpc

import com.google.gson.Gson
import com.google.gson.JsonElement
import java.util.concurrent.atomic.AtomicLong

/**
 * JSON-RPC 2.0 request for Ethereum RPC calls.
 */
data class RpcRequest(
    val jsonrpc: String = "2.0",
    val method: String,
    val params: List<Any?> = emptyList(),
    val id: Long = nextId()
) {
    companion object {
        private val idCounter = AtomicLong(1)
        private val gson = Gson()

        private fun nextId(): Long = idCounter.getAndIncrement()

        /**
         * Create a request for eth_chainId.
         */
        fun ethChainId(): RpcRequest = RpcRequest(method = "eth_chainId")

        /**
         * Create a request for eth_blockNumber.
         */
        fun ethBlockNumber(): RpcRequest = RpcRequest(method = "eth_blockNumber")

        /**
         * Create a request for eth_getBlockByNumber.
         * @param blockNumber Hex-encoded block number or "latest", "pending", "earliest"
         * @param includeTransactions Whether to include full transaction objects
         */
        fun ethGetBlockByNumber(blockNumber: String, includeTransactions: Boolean = false): RpcRequest =
            RpcRequest(method = "eth_getBlockByNumber", params = listOf(blockNumber, includeTransactions))

        /**
         * Create a request for eth_getBlockByNumber with numeric block number.
         */
        fun ethGetBlockByNumber(blockNumber: Long, includeTransactions: Boolean = false): RpcRequest =
            ethGetBlockByNumber("0x${blockNumber.toString(16)}", includeTransactions)

        /**
         * Create a request for eth_getBlockByHash.
         */
        fun ethGetBlockByHash(blockHash: String, includeTransactions: Boolean = false): RpcRequest =
            RpcRequest(method = "eth_getBlockByHash", params = listOf(blockHash, includeTransactions))

        /**
         * Create a request for eth_getTransactionByHash.
         */
        fun ethGetTransactionByHash(txHash: String): RpcRequest =
            RpcRequest(method = "eth_getTransactionByHash", params = listOf(txHash))

        /**
         * Create a request for eth_getTransactionReceipt.
         */
        fun ethGetTransactionReceipt(txHash: String): RpcRequest =
            RpcRequest(method = "eth_getTransactionReceipt", params = listOf(txHash))

        /**
         * Create a request for eth_getLogs.
         */
        fun ethGetLogs(
            fromBlock: String,
            toBlock: String,
            address: String? = null,
            topics: List<String?>? = null
        ): RpcRequest {
            val filter = mutableMapOf<String, Any?>(
                "fromBlock" to fromBlock,
                "toBlock" to toBlock
            )
            address?.let { filter["address"] = it }
            topics?.let { filter["topics"] = it }
            return RpcRequest(method = "eth_getLogs", params = listOf(filter))
        }

        /**
         * Create a request for eth_getBalance.
         */
        fun ethGetBalance(address: String, blockNumber: String = "latest"): RpcRequest =
            RpcRequest(method = "eth_getBalance", params = listOf(address, blockNumber))

        /**
         * Create a request for eth_getTransactionCount (nonce).
         */
        fun ethGetTransactionCount(address: String, blockNumber: String = "latest"): RpcRequest =
            RpcRequest(method = "eth_getTransactionCount", params = listOf(address, blockNumber))

        /**
         * Create a request for eth_getCode.
         */
        fun ethGetCode(address: String, blockNumber: String = "latest"): RpcRequest =
            RpcRequest(method = "eth_getCode", params = listOf(address, blockNumber))

        /**
         * Create a request for eth_gasPrice.
         */
        fun ethGasPrice(): RpcRequest = RpcRequest(method = "eth_gasPrice")

        /**
         * Create a request for web3_clientVersion.
         */
        fun web3ClientVersion(): RpcRequest = RpcRequest(method = "web3_clientVersion")
    }

    /**
     * Convert to JSON string for HTTP body.
     */
    fun toJson(): String = gson.toJson(this)
}
