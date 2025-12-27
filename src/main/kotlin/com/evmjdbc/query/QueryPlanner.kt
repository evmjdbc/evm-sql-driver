package com.evmjdbc.query

import com.evmjdbc.connection.ConnectionProperties
import com.evmjdbc.exceptions.RpcTranslationException
import com.evmjdbc.metadata.VirtualSchema
import com.evmjdbc.rpc.RpcRequest

/**
 * Converts parsed SQL queries into execution plans with RPC calls.
 */
class QueryPlanner(
    private val properties: ConnectionProperties
) {
    /**
     * Create an execution plan for the given parsed query.
     */
    fun plan(query: ParsedQuery): ExecutionPlan {
        // Validate table exists
        val tableDef = VirtualSchema.getTable(query.tableName)
            ?: throw RpcTranslationException.unsupportedTable(query.tableName)

        // Resolve SELECT expressions (* → all columns)
        val selectExpressions = if (query.isSelectAll()) {
            tableDef.columns.map { SelectExpression.Column(it.name) }
        } else {
            // Validate column references in expressions
            for (expr in query.selectExpressions) {
                for (colRef in expr.getColumnReferences()) {
                    if (tableDef.columns.none { it.name.equals(colRef, ignoreCase = true) }) {
                        throw RpcTranslationException("Unknown column: $colRef", query.tableName)
                    }
                }
            }
            query.selectExpressions
        }

        // Validate WHERE clause columns
        for (condition in query.conditions) {
            if (tableDef.columns.none { it.name.equals(condition.column, ignoreCase = true) }) {
                throw RpcTranslationException("Unknown column: ${condition.column}", query.tableName)
            }
        }

        // Validate ORDER BY columns
        for (orderItem in query.orderBy) {
            if (tableDef.columns.none { it.name.equals(orderItem.column, ignoreCase = true) }) {
                throw RpcTranslationException("Unknown column: ${orderItem.column}", query.tableName)
            }
        }

        return when (query.tableName) {
            "blocks" -> planBlocksQuery(query, selectExpressions)
            "transactions" -> planTransactionsQuery(query, selectExpressions)
            "logs" -> planLogsQuery(query, selectExpressions)
            "accounts" -> planAccountsQuery(query, selectExpressions)
            "erc20_transfers" -> planErc20TransfersQuery(query, selectExpressions)
            "erc721_transfers" -> planErc721TransfersQuery(query, selectExpressions)
            "chain_info" -> planChainInfoQuery(query, selectExpressions)
            "rpc_methods" -> planRpcMethodsQuery(query, selectExpressions)
            else -> throw RpcTranslationException.unsupportedTable(query.tableName)
        }
    }

    private fun planBlocksQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        val rpcCalls = mutableListOf<RpcCall>()
        val postFilters = mutableListOf<WhereCondition>()

        // Extract block number conditions
        val numberConditions = query.getConditions("number")
        val hashCondition = query.getCondition("hash")

        when {
            // Query by hash
            hashCondition != null && hashCondition.operator == ComparisonOperator.EQUALS -> {
                val hash = hashCondition.value as? String
                    ?: throw RpcTranslationException("Block hash must be a string")
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getBlockByHash",
                        request = RpcRequest.ethGetBlockByHash(hash, true),
                        purpose = "Get block by hash: $hash"
                    )
                )
            }

            // Query by exact block number
            numberConditions.any { it.operator == ComparisonOperator.EQUALS } -> {
                val eqCondition = numberConditions.first { it.operator == ComparisonOperator.EQUALS }
                val blockNum = (eqCondition.value as? Number)?.toLong()
                    ?: throw RpcTranslationException("Block number must be a number")
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getBlockByNumber",
                        request = RpcRequest.ethGetBlockByNumber(blockNum, true),
                        purpose = "Get block #$blockNum"
                    )
                )
            }

            // Query by block range - no hard limit, will batch execute
            numberConditions.isNotEmpty() -> {
                val range = extractBlockRange(numberConditions, query.limit)

                // Apply user's LIMIT to the range if specified to avoid unnecessary fetches
                val effectiveRange = if (query.limit != null && query.limit < range.size) {
                    BlockRange(range.from, range.from + query.limit - 1)
                } else {
                    range
                }

                for (blockNum in effectiveRange.toList()) {
                    rpcCalls.add(
                        RpcCall(
                            method = "eth_getBlockByNumber",
                            request = RpcRequest.ethGetBlockByNumber(blockNum, true),
                            purpose = "Get block #$blockNum"
                        )
                    )
                }

                // Add non-number conditions as post-filters
                query.conditions.filter { it.column != "number" }.forEach {
                    postFilters.add(it)
                }
            }

            // No number/hash conditions - default to latest block
            else -> {
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getBlockByNumber",
                        request = RpcRequest.ethGetBlockByNumber("latest", true),
                        purpose = "Get latest block"
                    )
                )

                // All conditions become post-filters
                query.conditions.forEach {
                    postFilters.add(it)
                }
            }
        }

        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = postFilters,
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun planTransactionsQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        val rpcCalls = mutableListOf<RpcCall>()
        val postFilters = mutableListOf<WhereCondition>()

        // Extract conditions
        val hashCondition = query.getCondition("hash")
        val blockNumberConditions = query.getConditions("block_number")

        when {
            // Query by transaction hash
            hashCondition != null && hashCondition.operator == ComparisonOperator.EQUALS -> {
                val hash = hashCondition.value as? String
                    ?: throw RpcTranslationException("Transaction hash must be a string")

                // Need both transaction and receipt for full data
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getTransactionByHash",
                        request = RpcRequest.ethGetTransactionByHash(hash),
                        purpose = "Get transaction: $hash"
                    )
                )
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getTransactionReceipt",
                        request = RpcRequest.ethGetTransactionReceipt(hash),
                        purpose = "Get receipt: $hash"
                    )
                )
            }

            // Query by block number - get block with transactions
            blockNumberConditions.any { it.operator == ComparisonOperator.EQUALS } -> {
                val eqCondition = blockNumberConditions.first { it.operator == ComparisonOperator.EQUALS }
                val blockNum = (eqCondition.value as? Number)?.toLong()
                    ?: throw RpcTranslationException("Block number must be a number")

                rpcCalls.add(
                    RpcCall(
                        method = "eth_getBlockByNumber",
                        request = RpcRequest.ethGetBlockByNumber(blockNum, true),
                        purpose = "Get block #$blockNum with transactions"
                    )
                )

                // Other conditions become post-filters
                query.conditions.filter { it.column != "block_number" }.forEach {
                    postFilters.add(it)
                }
            }

            // No hash or block_number - default to latest block's transactions
            else -> {
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getBlockByNumber",
                        request = RpcRequest.ethGetBlockByNumber("latest", true),
                        purpose = "Get latest block with transactions"
                    )
                )

                // All conditions become post-filters
                query.conditions.forEach {
                    postFilters.add(it)
                }
            }
        }

        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = postFilters,
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun planLogsQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        val rpcCalls = mutableListOf<RpcCall>()
        val postFilters = mutableListOf<WhereCondition>()

        // Extract block range conditions
        val blockNumberConditions = query.getConditions("block_number")

        // Extract address filter (normalize to lowercase for RPC)
        val addressCondition = query.getCondition("address")
        val address = if (addressCondition?.operator == ComparisonOperator.EQUALS) {
            (addressCondition.value as? String)?.lowercase()
        } else {
            addressCondition?.let { postFilters.add(it) }
            null
        }

        // Extract topic filters
        val topics = mutableListOf<String?>()
        for (i in 0..3) {
            val topicCondition = query.getCondition("topic$i")
            if (topicCondition?.operator == ComparisonOperator.EQUALS) {
                topics.add(topicCondition.value as? String)
            } else {
                topicCondition?.let { postFilters.add(it) }
                topics.add(null)
            }
        }
        // Trim trailing nulls from topics
        while (topics.isNotEmpty() && topics.last() == null) {
            topics.removeAt(topics.size - 1)
        }

        // Maximum blocks per eth_getLogs call (most nodes limit to 2048)
        val maxBlocksPerCall = 2000L
        val topicsParam = if (topics.isEmpty()) null else topics

        when {
            blockNumberConditions.isEmpty() -> {
                // No block filter - default to latest block for DataGrip compatibility
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getLogs",
                        request = RpcRequest.ethGetLogs(
                            fromBlock = "latest",
                            toBlock = "latest",
                            address = address,
                            topics = topicsParam
                        ),
                        purpose = "Get logs at latest block"
                    )
                )
            }
            blockNumberConditions.any { it.operator == ComparisonOperator.EQUALS } -> {
                val eqCondition = blockNumberConditions.first { it.operator == ComparisonOperator.EQUALS }
                val blockNum = (eqCondition.value as? Number)?.toLong()
                    ?: throw RpcTranslationException("Block number must be a number")
                val blockHex = "0x${blockNum.toString(16)}"
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getLogs",
                        request = RpcRequest.ethGetLogs(
                            fromBlock = blockHex,
                            toBlock = blockHex,
                            address = address,
                            topics = topicsParam
                        ),
                        purpose = "Get logs at block $blockNum"
                    )
                )
            }
            else -> {
                val range = extractBlockRange(blockNumberConditions, query.limit)

                // Split large ranges into chunks to avoid RPC limits
                var currentFrom = range.from
                while (currentFrom <= range.to) {
                    val currentTo = minOf(currentFrom + maxBlocksPerCall - 1, range.to)
                    val fromHex = "0x${currentFrom.toString(16)}"
                    val toHex = "0x${currentTo.toString(16)}"

                    rpcCalls.add(
                        RpcCall(
                            method = "eth_getLogs",
                            request = RpcRequest.ethGetLogs(
                                fromBlock = fromHex,
                                toBlock = toHex,
                                address = address,
                                topics = topicsParam
                            ),
                            purpose = "Get logs from $currentFrom to $currentTo"
                        )
                    )
                    currentFrom = currentTo + 1
                }
            }
        }

        // Add remaining conditions as post-filters
        query.conditions
            .filter { it.column !in listOf("block_number", "address", "topic0", "topic1", "topic2", "topic3") }
            .forEach { postFilters.add(it) }

        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = postFilters,
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun planAccountsQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        val rpcCalls = mutableListOf<RpcCall>()
        val postFilters = mutableListOf<WhereCondition>()

        // Extract address conditions
        val addressCondition = query.getCondition("address")
        val addressInCondition = query.conditions.find {
            it.column == "address" && it.operator == ComparisonOperator.IN
        }

        // Collect addresses to query
        val addresses = mutableListOf<String>()

        when {
            // Single address query
            addressCondition?.operator == ComparisonOperator.EQUALS -> {
                val addr = addressCondition.value as? String
                    ?: throw RpcTranslationException("Address must be a string")
                addresses.add(addr)
            }

            // Multiple addresses via IN clause
            addressInCondition != null -> {
                @Suppress("UNCHECKED_CAST")
                val addrList = addressInCondition.value as? List<Any>
                    ?: throw RpcTranslationException("IN clause requires a list of addresses")
                addresses.addAll(addrList.map { it.toString() })
            }

            // No address filter - error (can't query all accounts)
            else -> {
                throw RpcTranslationException.missingRequiredFilter("accounts", "address")
            }
        }

        // For each address, we need 3 RPC calls: balance, nonce, code
        // We'll use a special marker in the purpose to group them later
        for (address in addresses) {
            rpcCalls.add(
                RpcCall(
                    method = "eth_getBalance",
                    request = RpcRequest.ethGetBalance(address, "latest"),
                    purpose = "account:$address:balance"
                )
            )
            rpcCalls.add(
                RpcCall(
                    method = "eth_getTransactionCount",
                    request = RpcRequest.ethGetTransactionCount(address, "latest"),
                    purpose = "account:$address:nonce"
                )
            )
            rpcCalls.add(
                RpcCall(
                    method = "eth_getCode",
                    request = RpcRequest.ethGetCode(address, "latest"),
                    purpose = "account:$address:code"
                )
            )
        }

        // Add non-address conditions as post-filters
        query.conditions
            .filter { it.column != "address" }
            .forEach { postFilters.add(it) }

        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = postFilters,
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun planErc20TransfersQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        // ERC20 transfers are logs with Transfer topic
        // We reuse the logs planning but force topic0 filter
        return planTokenTransfersQuery(query, selectExpressions, "erc20_transfers")
    }

    private fun planErc721TransfersQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        // ERC721 transfers are logs with Transfer topic (same as ERC20)
        return planTokenTransfersQuery(query, selectExpressions, "erc721_transfers")
    }

    private fun planTokenTransfersQuery(
        query: ParsedQuery,
        selectExpressions: List<SelectExpression>,
        tableName: String
    ): ExecutionPlan {
        val rpcCalls = mutableListOf<RpcCall>()
        val postFilters = mutableListOf<WhereCondition>()

        // Transfer event topic: keccak256("Transfer(address,address,uint256)")
        val transferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        // Extract block range conditions
        val blockNumberConditions = query.getConditions("block_number")

        // Extract token address filter (maps to log address)
        // Normalize to lowercase for RPC call (case-insensitive matching)
        val tokenAddressCondition = query.getCondition("token_address")
        val tokenAddress = if (tokenAddressCondition?.operator == ComparisonOperator.EQUALS) {
            (tokenAddressCondition.value as? String)?.lowercase()
        } else {
            tokenAddressCondition?.let { postFilters.add(it) }
            null
        }

        // Extract from_address filter (maps to topic1)
        val fromCondition = query.getCondition("from_address")
        val fromTopic = if (fromCondition?.operator == ComparisonOperator.EQUALS) {
            val addr = (fromCondition.value as? String)?.lowercase() ?: ""
            // Pad address to 32 bytes
            "0x" + addr.removePrefix("0x").padStart(64, '0')
        } else {
            fromCondition?.let { postFilters.add(it) }
            null
        }

        // Extract to_address filter (maps to topic2)
        val toCondition = query.getCondition("to_address")
        val toTopic = if (toCondition?.operator == ComparisonOperator.EQUALS) {
            val addr = (toCondition.value as? String)?.lowercase() ?: ""
            "0x" + addr.removePrefix("0x").padStart(64, '0')
        } else {
            toCondition?.let { postFilters.add(it) }
            null
        }

        // Build topics array: [topic0, from, to]
        val topics = mutableListOf<String?>(transferTopic, fromTopic, toTopic)
        // Trim trailing nulls
        while (topics.isNotEmpty() && topics.last() == null) {
            topics.removeAt(topics.size - 1)
        }

        // Maximum blocks per eth_getLogs call (most nodes limit to 2048)
        val maxBlocksPerCall = 2000L

        when {
            blockNumberConditions.isEmpty() -> {
                // Default to latest block
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getLogs",
                        request = RpcRequest.ethGetLogs(
                            fromBlock = "latest",
                            toBlock = "latest",
                            address = tokenAddress,
                            topics = topics
                        ),
                        purpose = "Get $tableName at latest block"
                    )
                )
            }
            blockNumberConditions.any { it.operator == ComparisonOperator.EQUALS } -> {
                val eqCondition = blockNumberConditions.first { it.operator == ComparisonOperator.EQUALS }
                val blockNum = (eqCondition.value as? Number)?.toLong()
                    ?: throw RpcTranslationException("Block number must be a number")
                val blockHex = "0x${blockNum.toString(16)}"
                rpcCalls.add(
                    RpcCall(
                        method = "eth_getLogs",
                        request = RpcRequest.ethGetLogs(
                            fromBlock = blockHex,
                            toBlock = blockHex,
                            address = tokenAddress,
                            topics = topics
                        ),
                        purpose = "Get $tableName at block $blockNum"
                    )
                )
            }
            else -> {
                val range = extractBlockRange(blockNumberConditions, query.limit)

                // Split large ranges into chunks to avoid RPC limits
                var currentFrom = range.from
                while (currentFrom <= range.to) {
                    val currentTo = minOf(currentFrom + maxBlocksPerCall - 1, range.to)
                    val fromHex = "0x${currentFrom.toString(16)}"
                    val toHex = "0x${currentTo.toString(16)}"

                    rpcCalls.add(
                        RpcCall(
                            method = "eth_getLogs",
                            request = RpcRequest.ethGetLogs(
                                fromBlock = fromHex,
                                toBlock = toHex,
                                address = tokenAddress,
                                topics = topics
                            ),
                            purpose = "Get $tableName from $currentFrom to $currentTo"
                        )
                    )
                    currentFrom = currentTo + 1
                }
            }
        }

        // Add remaining conditions as post-filters
        query.conditions
            .filter { it.column !in listOf("block_number", "token_address", "from_address", "to_address") }
            .forEach { postFilters.add(it) }

        return ExecutionPlan(
            tableName = tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = postFilters,
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun planChainInfoQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        val rpcCalls = listOf(
            RpcCall(
                method = "eth_chainId",
                request = RpcRequest.ethChainId(),
                purpose = "Get chain ID"
            ),
            RpcCall(
                method = "eth_blockNumber",
                request = RpcRequest.ethBlockNumber(),
                purpose = "Get latest block number"
            ),
            RpcCall(
                method = "eth_gasPrice",
                request = RpcRequest.ethGasPrice(),
                purpose = "Get current gas price"
            )
        )

        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = rpcCalls,
            selectExpressions = selectExpressions,
            postFilters = emptyList(),
            orderBy = emptyList(),
            limit = 1,
            distinct = query.distinct
        )
    }

    private fun planRpcMethodsQuery(query: ParsedQuery, selectExpressions: List<SelectExpression>): ExecutionPlan {
        // Static data, no RPC calls needed
        return ExecutionPlan(
            tableName = query.tableName,
            rpcCalls = emptyList(),
            selectExpressions = selectExpressions,
            postFilters = emptyList(),
            orderBy = query.orderBy,
            limit = query.limit,
            distinct = query.distinct
        )
    }

    private fun extractBlockRange(conditions: List<WhereCondition>, limit: Long?): BlockRange {
        var from: Long? = null
        var to: Long? = null

        for (condition in conditions) {
            val value = (condition.value as? Number)?.toLong() ?: continue

            when (condition.operator) {
                ComparisonOperator.EQUALS -> {
                    from = value
                    to = value
                }
                ComparisonOperator.GREATER_THAN -> from = value + 1
                ComparisonOperator.GREATER_THAN_EQUALS -> from = value
                ComparisonOperator.LESS_THAN -> to = value - 1
                ComparisonOperator.LESS_THAN_EQUALS -> to = value
                else -> {}
            }
        }

        // Apply limit if no upper bound
        if (from != null && to == null && limit != null) {
            to = from + limit - 1
        }

        if (from == null || to == null) {
            throw RpcTranslationException(
                "Block range query requires both lower and upper bounds, or use LIMIT",
                "blocks", "number"
            )
        }

        return BlockRange(from, to)
    }

    private fun validateBlockRange(range: BlockRange) {
        if (range.size > properties.maxBlockRange) {
            throw RpcTranslationException.blockRangeExceeded(range.size, properties.maxBlockRange.toLong())
        }
        if (range.size < 0) {
            throw RpcTranslationException("Invalid block range: from ${range.from} to ${range.to}")
        }
    }
}
