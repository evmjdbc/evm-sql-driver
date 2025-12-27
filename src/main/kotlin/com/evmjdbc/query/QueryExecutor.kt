package com.evmjdbc.query

import com.evmjdbc.exceptions.RpcTranslationException
import com.evmjdbc.rpc.RpcClient
import com.evmjdbc.types.AccountData
import com.evmjdbc.types.BlockData
import com.evmjdbc.types.Erc20TransferData
import com.evmjdbc.types.Erc721TransferData
import com.evmjdbc.types.LogData
import com.evmjdbc.types.TransactionData
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors

/**
 * Executes query plans by making RPC calls and transforming results.
 * Also supports UNION and JOIN operations via in-memory processing.
 */
class QueryExecutor(
    private val rpcClient: RpcClient,
    private val queryPlanner: QueryPlanner? = null
) {
    private val logger = LoggerFactory.getLogger(QueryExecutor::class.java)

    // Secondary constructor for backwards compatibility
    constructor(rpcClient: RpcClient) : this(rpcClient, null)

    /**
     * Execute an execution plan and return rows.
     * @return Pair of (column names, rows data)
     */
    fun execute(plan: ExecutionPlan): QueryResult {
        logger.debug("Executing plan for table: {} with {} RPC calls", plan.tableName, plan.rpcCalls.size)

        return when (plan.tableName) {
            "blocks" -> executeBlocksQuery(plan)
            "transactions" -> executeTransactionsQuery(plan)
            "logs" -> executeLogsQuery(plan)
            "accounts" -> executeAccountsQuery(plan)
            "erc20_transfers" -> executeErc20TransfersQuery(plan)
            "erc721_transfers" -> executeErc721TransfersQuery(plan)
            "chain_info" -> executeChainInfoQuery(plan)
            "rpc_methods" -> executeRpcMethodsQuery(plan)
            else -> throw RpcTranslationException("Table not yet implemented: ${plan.tableName}")
        }
    }

    private fun executeBlocksQuery(plan: ExecutionPlan): QueryResult {
        val blocks = mutableListOf<BlockData>()
        val totalCalls = plan.rpcCalls.size

        if (totalCalls == 0) {
            return QueryResult(plan.getOutputColumnNames(), emptyList())
        }

        // Use parallel execution for large queries
        if (totalCalls > PARALLEL_THRESHOLD) {
            logger.info("Executing {} block queries in parallel batches", totalCalls)
            val executor = Executors.newFixedThreadPool(PARALLEL_THREADS)
            try {
                val futures = plan.rpcCalls.map { call ->
                    CompletableFuture.supplyAsync({
                        val response = rpcClient.execute(call.request)
                        if (response.isError()) {
                            throw SQLException("RPC error: ${response.error?.message}")
                        }
                        response.resultAsObject()?.let { BlockData.fromJson(it) }
                    }, executor)
                }

                // Collect results, logging progress for large queries
                var completed = 0
                for (future in futures) {
                    future.get()?.let { blocks.add(it) }
                    completed++
                    if (completed % 1000 == 0) {
                        logger.info("Progress: {}/{} blocks fetched", completed, totalCalls)
                    }
                }
            } finally {
                executor.shutdown()
            }
        } else {
            // Sequential execution for small queries
            for (call in plan.rpcCalls) {
                logger.debug("Executing: {}", call.purpose)
                val response = rpcClient.execute(call.request)

                if (response.isError()) {
                    throw SQLException("RPC error: ${response.error?.message}")
                }

                val result = response.resultAsObject()
                if (result != null) {
                    blocks.add(BlockData.fromJson(result))
                }
            }
        }

        // Apply post-filters
        val filtered = blocks.filter { block ->
            plan.postFilters.all { condition ->
                matchesCondition(block.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { block: BlockData, column: String ->
            block.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { block ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> block.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun executeTransactionsQuery(plan: ExecutionPlan): QueryResult {
        val transactions = mutableListOf<TransactionData>()

        // Check if we're querying by transaction hash or by block
        val hasBlockCall = plan.rpcCalls.any { it.method == "eth_getBlockByNumber" }

        if (hasBlockCall) {
            // Query by block - transactions are embedded in block
            for (call in plan.rpcCalls) {
                if (call.method == "eth_getBlockByNumber") {
                    logger.debug("Executing: {}", call.purpose)
                    val response = rpcClient.execute(call.request)

                    if (response.isError()) {
                        throw SQLException("RPC error: ${response.error?.message}")
                    }

                    val blockResult = response.resultAsObject()
                    if (blockResult != null) {
                        val txArray = blockResult.get("transactions")?.asJsonArray
                        txArray?.forEach { txElement ->
                            if (txElement.isJsonObject) {
                                transactions.add(TransactionData.fromJson(txElement.asJsonObject))
                            }
                        }
                    }
                }
            }
        } else {
            // Query by transaction hash - need to pair tx with receipt
            val txCalls = plan.rpcCalls.filter { it.method == "eth_getTransactionByHash" }
            val receiptCalls = plan.rpcCalls.filter { it.method == "eth_getTransactionReceipt" }

            for (i in txCalls.indices) {
                logger.debug("Executing: {}", txCalls[i].purpose)
                val txResponse = rpcClient.execute(txCalls[i].request)
                val receiptResponse = if (i < receiptCalls.size) {
                    logger.debug("Executing: {}", receiptCalls[i].purpose)
                    rpcClient.execute(receiptCalls[i].request)
                } else null

                if (txResponse.isError()) {
                    throw SQLException("RPC error: ${txResponse.error?.message}")
                }

                val txResult = txResponse.resultAsObject()
                val receiptResult = receiptResponse?.resultAsObject()

                if (txResult != null) {
                    transactions.add(TransactionData.fromJson(txResult, receiptResult))
                }
            }
        }

        // Apply post-filters
        val filtered = transactions.filter { tx ->
            plan.postFilters.all { condition ->
                matchesCondition(tx.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { tx: TransactionData, column: String ->
            tx.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { tx ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> tx.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun executeLogsQuery(plan: ExecutionPlan): QueryResult {
        val logs = mutableListOf<LogData>()

        for (call in plan.rpcCalls) {
            logger.debug("Executing: {}", call.purpose)
            val response = rpcClient.execute(call.request)

            if (response.isError()) {
                throw SQLException("RPC error: ${response.error?.message}")
            }

            // eth_getLogs returns an array of log objects
            val logArray = response.resultAsList()
            for (logJson in logArray) {
                logs.add(LogData.fromJson(logJson))
            }
        }

        // Apply post-filters
        val filtered = logs.filter { log ->
            plan.postFilters.all { condition ->
                matchesCondition(log.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { log: LogData, column: String ->
            log.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { log ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> log.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun executeAccountsQuery(plan: ExecutionPlan): QueryResult {
        // Group RPC calls by address (each address has 3 calls: balance, nonce, code)
        val accountResults = mutableMapOf<String, MutableMap<String, String?>>()

        // Execute all calls and collect results by address
        for (call in plan.rpcCalls) {
            logger.debug("Executing: {}", call.purpose)
            val response = rpcClient.execute(call.request)

            if (response.isError()) {
                throw SQLException("RPC error: ${response.error?.message}")
            }

            // Parse purpose: "account:0x...:balance|nonce|code"
            val parts = call.purpose.split(":")
            if (parts.size >= 3) {
                val address = parts[1]
                val field = parts[2]
                val value = response.resultAsString()

                accountResults.getOrPut(address) { mutableMapOf() }[field] = value
            }
        }

        // Build AccountData objects from collected results
        val accounts = accountResults.map { (address, fields) ->
            AccountData.fromRpcResponses(
                address = address,
                balanceHex = fields["balance"],
                nonceHex = fields["nonce"],
                code = fields["code"]
            )
        }

        // Apply post-filters
        val filtered = accounts.filter { account ->
            plan.postFilters.all { condition ->
                matchesCondition(account.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { account: AccountData, column: String ->
            account.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { account ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> account.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun executeErc20TransfersQuery(plan: ExecutionPlan): QueryResult {
        // First fetch logs, then decode as ERC20 transfers
        val logs = fetchLogs(plan)

        // Decode logs as ERC20 transfers (filter out non-matching)
        val transfers = logs.mapNotNull { log -> Erc20TransferData.fromLog(log) }

        // Apply post-filters
        val filtered = transfers.filter { transfer ->
            plan.postFilters.all { condition ->
                matchesCondition(transfer.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { transfer: Erc20TransferData, column: String ->
            transfer.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { transfer ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> transfer.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun executeErc721TransfersQuery(plan: ExecutionPlan): QueryResult {
        // First fetch logs, then decode as ERC721 transfers
        val logs = fetchLogs(plan)

        // Decode logs as ERC721 transfers (filter out non-matching)
        val transfers = logs.mapNotNull { log -> Erc721TransferData.fromLog(log) }

        // Apply post-filters
        val filtered = transfers.filter { transfer ->
            plan.postFilters.all { condition ->
                matchesCondition(transfer.getValue(condition.column), condition)
            }
        }

        // Apply ordering
        val ordered = applyOrdering(filtered, plan.orderBy) { transfer: Erc721TransferData, column: String ->
            transfer.getValue(column)
        }

        // Evaluate expressions to build result rows
        val rows = ordered.map { transfer ->
            plan.selectExpressions.map { expr -> evaluateExpression(expr) { col -> transfer.getValue(col) } }
        }

        // Apply DISTINCT if requested
        val distincted = if (plan.distinct) {
            rows.distinctBy { row: List<Any?> -> row.map { it?.toString() ?: "NULL" } }
        } else rows

        // Apply limit
        val limited = plan.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    /**
     * Fetch logs from RPC calls in the execution plan.
     * Uses parallel execution when there are multiple calls.
     */
    private fun fetchLogs(plan: ExecutionPlan): List<LogData> {
        val calls = plan.rpcCalls.filter { it.method == "eth_getLogs" }

        if (calls.size <= 1) {
            // Single call - execute directly
            val logs = mutableListOf<LogData>()
            for (call in calls) {
                logger.debug("Executing: {}", call.purpose)
                val response = rpcClient.execute(call.request)

                if (response.isError()) {
                    throw SQLException("RPC error: ${response.error?.message}")
                }

                val logArray = response.resultAsList()
                for (logJson in logArray) {
                    logs.add(LogData.fromJson(logJson))
                }
            }
            return logs
        }

        // Multiple calls - execute in parallel
        logger.debug("Executing {} eth_getLogs calls in parallel", calls.size)
        val executor = Executors.newFixedThreadPool(minOf(PARALLEL_THREADS, calls.size))

        try {
            val futures = calls.map { call ->
                CompletableFuture.supplyAsync({
                    logger.debug("Executing: {}", call.purpose)
                    val response = rpcClient.execute(call.request)
                    if (response.isError()) {
                        throw RuntimeException("RPC error: ${response.error?.message}")
                    }
                    response.resultAsList().map { LogData.fromJson(it) }
                }, executor)
            }

            // Collect all results in order
            return futures.flatMap { future ->
                try {
                    future.get()
                } catch (e: Exception) {
                    throw SQLException("Failed to fetch logs: ${e.cause?.message ?: e.message}")
                }
            }
        } finally {
            executor.shutdown()
        }
    }

    private fun executeChainInfoQuery(plan: ExecutionPlan): QueryResult {
        var chainId: Long = 0
        var latestBlock: Long = 0
        var gasPrice: BigDecimal = BigDecimal.ZERO

        for (call in plan.rpcCalls) {
            logger.debug("Executing: {}", call.purpose)
            val response = rpcClient.execute(call.request)

            if (response.isError()) {
                throw SQLException("RPC error: ${response.error?.message}")
            }

            when (call.method) {
                "eth_chainId" -> {
                    val hex = response.resultAsString()
                    chainId = hex?.removePrefix("0x")?.toLongOrNull(16) ?: 0
                }
                "eth_blockNumber" -> {
                    val hex = response.resultAsString()
                    latestBlock = hex?.removePrefix("0x")?.toLongOrNull(16) ?: 0
                }
                "eth_gasPrice" -> {
                    val hex = response.resultAsString()
                    gasPrice = hex?.removePrefix("0x")?.let {
                        BigDecimal(java.math.BigInteger(it, 16))
                    } ?: BigDecimal.ZERO
                }
            }
        }

        val networkName = getNetworkName(chainId)

        // Build column value map
        val columnValues = mapOf(
            "chain_id" to chainId,
            "network_name" to networkName,
            "latest_block" to latestBlock,
            "gas_price" to gasPrice,
            "client_version" to null
        )

        // Evaluate expressions
        val row = plan.selectExpressions.map { expr ->
            evaluateExpression(expr) { col -> columnValues[col.lowercase()] }
        }

        return QueryResult(plan.getOutputColumnNames(), listOf(row))
    }

    private fun executeRpcMethodsQuery(plan: ExecutionPlan): QueryResult {
        // Static data - no RPC calls needed
        val methods = listOf(
            mapOf("method_name" to "eth_chainId", "supported" to true, "description" to "Returns the chain ID"),
            mapOf("method_name" to "eth_blockNumber", "supported" to true, "description" to "Returns the current block number"),
            mapOf("method_name" to "eth_gasPrice", "supported" to true, "description" to "Returns the current gas price"),
            mapOf("method_name" to "eth_getBlockByNumber", "supported" to true, "description" to "Returns block by number"),
            mapOf("method_name" to "eth_getBlockByHash", "supported" to true, "description" to "Returns block by hash"),
            mapOf("method_name" to "eth_getTransactionByHash", "supported" to true, "description" to "Returns transaction by hash"),
            mapOf("method_name" to "eth_getTransactionReceipt", "supported" to true, "description" to "Returns transaction receipt"),
            mapOf("method_name" to "eth_getLogs", "supported" to true, "description" to "Returns logs matching filter"),
            mapOf("method_name" to "eth_getBalance", "supported" to true, "description" to "Returns account balance"),
            mapOf("method_name" to "eth_getCode", "supported" to true, "description" to "Returns contract code"),
            mapOf("method_name" to "eth_call", "supported" to true, "description" to "Executes a call (read-only)"),
            mapOf("method_name" to "eth_estimateGas", "supported" to false, "description" to "Estimates gas for a transaction"),
            mapOf("method_name" to "eth_sendRawTransaction", "supported" to false, "description" to "Sends a signed transaction")
        )

        val rows = methods.map { method ->
            plan.selectExpressions.map { expr ->
                evaluateExpression(expr) { col -> method[col.lowercase()] }
            }
        }

        // Apply limit
        val limited = plan.limit?.let { rows.take(it.toInt()) } ?: rows

        return QueryResult(plan.getOutputColumnNames(), limited)
    }

    private fun matchesCondition(value: Any?, condition: WhereCondition): Boolean {
        if (value == null && condition.operator !in listOf(ComparisonOperator.IS_NULL, ComparisonOperator.IS_NOT_NULL)) {
            return false
        }

        return when (condition.operator) {
            ComparisonOperator.EQUALS -> compareValues(value, condition.value) == 0
            ComparisonOperator.NOT_EQUALS -> compareValues(value, condition.value) != 0
            ComparisonOperator.GREATER_THAN -> compareValues(value, condition.value) > 0
            ComparisonOperator.GREATER_THAN_EQUALS -> compareValues(value, condition.value) >= 0
            ComparisonOperator.LESS_THAN -> compareValues(value, condition.value) < 0
            ComparisonOperator.LESS_THAN_EQUALS -> compareValues(value, condition.value) <= 0
            ComparisonOperator.IN -> {
                val values = condition.value as? List<*> ?: return false
                values.any { compareValues(value, it) == 0 }
            }
            ComparisonOperator.IS_NULL -> value == null
            ComparisonOperator.IS_NOT_NULL -> value != null
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareValues(a: Any?, b: Any?): Int {
        if (a == null && b == null) return 0
        if (a == null) return -1
        if (b == null) return 1

        return when {
            a is Number && b is Number -> {
                val aDouble = a.toDouble()
                val bDouble = b.toDouble()
                aDouble.compareTo(bDouble)
            }
            a is String && b is String -> a.compareTo(b, ignoreCase = true)
            a is Comparable<*> && b is Comparable<*> -> {
                try {
                    (a as Comparable<Any>).compareTo(b)
                } catch (e: ClassCastException) {
                    a.toString().compareTo(b.toString())
                }
            }
            else -> a.toString().compareTo(b.toString())
        }
    }

    private fun <T> applyOrdering(
        items: List<T>,
        orderBy: List<OrderByItem>,
        getValue: (T, String) -> Any?
    ): List<T> {
        if (orderBy.isEmpty()) return items

        return items.sortedWith { a, b ->
            for (order in orderBy) {
                val aVal = getValue(a, order.column)
                val bVal = getValue(b, order.column)
                val cmp = compareValues(aVal, bVal)
                if (cmp != 0) {
                    return@sortedWith if (order.ascending) cmp else -cmp
                }
            }
            0
        }
    }

    /**
     * Evaluate a SelectExpression against a row's column values.
     */
    private fun evaluateExpression(expr: SelectExpression, getColumnValue: (String) -> Any?): Any? {
        return when (expr) {
            is SelectExpression.Column -> getColumnValue(expr.name)
            is SelectExpression.Literal -> expr.value
            is SelectExpression.Arithmetic -> {
                val leftVal = evaluateExpression(expr.left, getColumnValue)
                val rightVal = evaluateExpression(expr.right, getColumnValue)
                evaluateArithmetic(leftVal, rightVal, expr.operator)
            }
            is SelectExpression.Function -> {
                // Basic function support
                val args = expr.arguments.map { evaluateExpression(it, getColumnValue) }
                evaluateFunction(expr.name, args)
            }
        }
    }

    /**
     * Evaluate an arithmetic operation.
     */
    private fun evaluateArithmetic(left: Any?, right: Any?, operator: ArithmeticOperator): Any? {
        if (left == null || right == null) return null

        val leftNum = toNumber(left)
        val rightNum = toNumber(right)

        if (leftNum == null || rightNum == null) return null

        return when (operator) {
            ArithmeticOperator.ADD -> leftNum + rightNum
            ArithmeticOperator.SUBTRACT -> leftNum - rightNum
            ArithmeticOperator.MULTIPLY -> leftNum * rightNum
            ArithmeticOperator.DIVIDE -> {
                if (rightNum == BigDecimal.ZERO) null
                else leftNum.divide(rightNum, 18, java.math.RoundingMode.HALF_UP)
            }
            ArithmeticOperator.MODULO -> {
                if (rightNum == BigDecimal.ZERO) null
                else leftNum.remainder(rightNum)
            }
        }
    }

    /**
     * Convert a value to BigDecimal for arithmetic.
     */
    private fun toNumber(value: Any?): BigDecimal? {
        return when (value) {
            null -> null
            is BigDecimal -> value
            is Long -> BigDecimal(value)
            is Int -> BigDecimal(value)
            is Double -> BigDecimal(value)
            is Float -> BigDecimal(value.toDouble())
            is String -> value.toBigDecimalOrNull()
            is Number -> BigDecimal(value.toDouble())
            else -> null
        }
    }

    /**
     * Evaluate a function call.
     */
    private fun evaluateFunction(name: String, args: List<Any?>): Any? {
        return when (name.lowercase()) {
            "coalesce" -> args.firstOrNull { it != null }
            "nullif" -> if (args.size >= 2 && args[0] == args[1]) null else args.getOrNull(0)
            "abs" -> args.getOrNull(0)?.let { toNumber(it)?.abs() }
            "upper" -> args.getOrNull(0)?.toString()?.uppercase()
            "lower" -> args.getOrNull(0)?.toString()?.lowercase()
            "length" -> args.getOrNull(0)?.toString()?.length
            "trim" -> args.getOrNull(0)?.toString()?.trim()
            else -> null // Unknown function returns null
        }
    }

    private fun getNetworkName(chainId: Long): String = when (chainId) {
        1L -> "Ethereum Mainnet"
        5L -> "Goerli"
        11155111L -> "Sepolia"
        137L -> "Polygon Mainnet"
        80001L -> "Polygon Mumbai"
        42161L -> "Arbitrum One"
        421614L -> "Arbitrum Sepolia"
        10L -> "Optimism"
        11155420L -> "Optimism Sepolia"
        8453L -> "Base"
        84532L -> "Base Sepolia"
        43114L -> "Avalanche C-Chain"
        43113L -> "Avalanche Fuji"
        56L -> "BNB Smart Chain"
        97L -> "BNB Testnet"
        100L -> "Gnosis Chain"
        250L -> "Fantom Opera"
        1101L -> "Polygon zkEVM"
        324L -> "zkSync Era"
        59144L -> "Linea"
        534352L -> "Scroll"
        else -> "Chain ID: $chainId"
    }

    /**
     * Execute a QueryNode (simple query, UNION, JOIN, dual, subquery, or CTE).
     * Requires a QueryPlanner to be set for most query types.
     */
    fun executeQuery(query: QueryNode): QueryResult {
        return when (query) {
            is DualQuery -> executeDualQuery(query)
            is ParsedQuery -> {
                val planner = queryPlanner ?: throw SQLException("QueryPlanner required for executeQuery")
                val plan = planner.plan(query)
                execute(plan)
            }
            is CompositeQuery -> {
                val planner = queryPlanner ?: throw SQLException("QueryPlanner required for executeQuery")
                executeCompositeQuery(query, planner)
            }
            is JoinedQuery -> {
                val planner = queryPlanner ?: throw SQLException("QueryPlanner required for executeQuery")
                executeJoinedQuery(query, planner)
            }
            is SubqueryQuery -> executeSubqueryQuery(query)
            is CteQuery -> executeCteQuery(query)
        }
    }

    /**
     * Execute a subquery-based query.
     * First executes the subquery, then applies outer query operations on the result.
     */
    private fun executeSubqueryQuery(query: SubqueryQuery): QueryResult {
        logger.debug("Executing subquery with alias: {}", query.alias)

        // First, execute the inner subquery
        val innerResult = executeQuery(query.subquery)

        // Build a map for each row: column name -> value
        val innerRows = innerResult.rows.map { row ->
            innerResult.columnNames.zip(row).toMap()
        }

        // Apply outer query WHERE conditions (post-filter on subquery results)
        val filtered = innerRows.filter { row ->
            query.conditions.all { condition ->
                matchesCondition(row[condition.column], condition)
            }
        }

        // Determine output columns from select expressions
        val outputColumnNames = mutableListOf<String>()
        val rows = filtered.map { row ->
            query.selectExpressions.flatMap { expr ->
                when (expr) {
                    is SelectExpression.Column -> {
                        if (expr.name == "*") {
                            // Add all columns from inner result
                            if (outputColumnNames.isEmpty() || outputColumnNames.size < innerResult.columnNames.size) {
                                outputColumnNames.clear()
                                outputColumnNames.addAll(innerResult.columnNames.map { expr.alias ?: it })
                            }
                            row.values.toList()
                        } else {
                            if (!outputColumnNames.contains(expr.getOutputName())) {
                                outputColumnNames.add(expr.getOutputName())
                            }
                            listOf(row[expr.name])
                        }
                    }
                    else -> {
                        if (!outputColumnNames.contains(expr.getOutputName())) {
                            outputColumnNames.add(expr.getOutputName())
                        }
                        listOf(evaluateExpression(expr) { col -> row[col] })
                    }
                }
            }
        }

        // Apply ordering
        val ordered = if (query.orderBy.isNotEmpty()) {
            applyOrderingToRows(rows, query.orderBy, outputColumnNames)
        } else {
            rows
        }

        // Apply DISTINCT if requested
        val distincted = if (query.distinct) {
            ordered.distinctBy { row -> row.map { it?.toString() ?: "NULL" } }
        } else {
            ordered
        }

        // Apply limit
        val limited = query.limit?.let { distincted.take(it.toInt()) } ?: distincted

        return QueryResult(outputColumnNames, limited)
    }

    /**
     * Execute a CTE query.
     * First evaluates all CTEs, then executes the main query.
     */
    private fun executeCteQuery(query: CteQuery): QueryResult {
        logger.debug("Executing CTE query with {} CTEs", query.ctes.size)

        // Execute each CTE and cache results
        val cteResults = mutableMapOf<String, QueryResult>()
        for (cte in query.ctes) {
            logger.debug("Executing CTE: {}", cte.name)
            cteResults[cte.name] = executeQuery(cte.query)
        }

        // Execute the main query
        // The main query will reference CTEs which have already been expanded during parsing
        return executeQuery(query.mainQuery)
    }

    /**
     * Execute a dual query (SELECT without FROM).
     * Returns constant values directly.
     */
    private fun executeDualQuery(query: DualQuery): QueryResult {
        val columnNames = query.values.map { it.first }
        val row = query.values.map { it.second }
        return QueryResult(columnNames, listOf(row))
    }

    /**
     * Execute a UNION/UNION ALL query.
     */
    private fun executeCompositeQuery(query: CompositeQuery, planner: QueryPlanner): QueryResult {
    if (query.queries.isEmpty()) {
        throw RpcTranslationException("Empty composite query")
    }

    // Execute each sub-query
    val subResults = query.queries.map { subQuery -> executeQuery(subQuery) }

    // Verify column counts match
    val firstColumns = subResults.first().columnNames
    for ((idx, result) in subResults.withIndex()) {
        if (result.columnNames.size != firstColumns.size) {
            throw RpcTranslationException(
                "UNION query mismatch: query ${idx + 1} has ${result.columnNames.size} columns, expected ${firstColumns.size}"
            )
        }
    }

    // Combine results based on operations
    var combinedRows = subResults.first().rows.toMutableList()

    for (i in 1 until subResults.size) {
        val rightRows = subResults[i].rows
        val operation = query.operations.getOrElse(i - 1) { SetOperation.UNION }

        when (operation) {
            SetOperation.UNION_ALL -> {
                // Simply append all rows
                combinedRows.addAll(rightRows)
            }
            SetOperation.UNION -> {
                // Append rows and remove duplicates
                combinedRows.addAll(rightRows)
                combinedRows = combinedRows.distinctBy { row -> row.map { it?.toString() ?: "NULL" } }.toMutableList()
            }
            SetOperation.INTERSECT -> {
                // Keep only rows that appear in both
                val rightSet = rightRows.map { row -> row.map { it?.toString() ?: "NULL" } }.toSet()
                combinedRows = combinedRows.filter { row ->
                    row.map { it?.toString() ?: "NULL" } in rightSet
                }.toMutableList()
            }
            SetOperation.EXCEPT -> {
                // Remove rows that appear in right
                val rightSet = rightRows.map { row -> row.map { it?.toString() ?: "NULL" } }.toSet()
                combinedRows = combinedRows.filter { row ->
                    row.map { it?.toString() ?: "NULL" } !in rightSet
                }.toMutableList()
            }
        }
    }

    // Apply global ORDER BY
    val ordered = if (query.orderBy.isNotEmpty()) {
        applyOrderingToRows(combinedRows, query.orderBy, firstColumns)
    } else {
        combinedRows
    }

    // Apply global LIMIT
    val limited = query.limit?.let { ordered.take(it.toInt()) } ?: ordered

    return QueryResult(firstColumns, limited)
}

/**
 * Execute a JOIN query.
 */
private fun executeJoinedQuery(query: JoinedQuery, planner: QueryPlanner): QueryResult {
    logger.debug("Executing JOIN query with tables: {}", query.getAllTables())

    // Execute query for left table with its conditions
    val leftConditions = query.getConditionsForTable(query.leftTable)
    val leftQuery = ParsedQuery(
        tableName = query.leftTable,
        selectExpressions = listOf(SelectExpression.Column("*")),
        conditions = leftConditions,
        limit = null, // Don't limit sub-queries
        orderBy = emptyList()
    )
    val leftPlan = planner.plan(leftQuery)
    val leftResult = execute(leftPlan)

    // Build map of left table data indexed by column values
    var joinedRows: List<Map<String, Any?>> = leftResult.rows.map { row ->
        leftResult.columnNames.zip(row).toMap().mapKeys { "${query.leftTable}.${it.key}" }
    }

    // Process each join
    for (joinInfo in query.joins) {
        val rightConditions = query.getConditionsForTable(joinInfo.rightTable)
        val rightQuery = ParsedQuery(
            tableName = joinInfo.rightTable,
            selectExpressions = listOf(SelectExpression.Column("*")),
            conditions = rightConditions,
            limit = null,
            orderBy = emptyList()
        )
        val rightPlan = planner.plan(rightQuery)
        val rightResult = execute(rightPlan)

        // Build right table data map
        val rightRows = rightResult.rows.map { row ->
            rightResult.columnNames.zip(row).toMap().mapKeys { "${joinInfo.rightTable}.${it.key}" }
        }

        // Perform the join
        joinedRows = when (joinInfo.joinType) {
            JoinType.INNER -> innerJoin(joinedRows, rightRows, joinInfo)
            JoinType.LEFT -> leftJoin(joinedRows, rightRows, joinInfo)
            JoinType.RIGHT -> rightJoin(joinedRows, rightRows, joinInfo)
            JoinType.FULL -> fullJoin(joinedRows, rightRows, joinInfo)
            JoinType.CROSS -> crossJoin(joinedRows, rightRows)
            JoinType.NATURAL -> naturalJoin(joinedRows, rightRows, joinInfo.rightTable)
        }
    }

    // Build result columns
    val resultColumns = mutableListOf<String>()
    for (qCol in query.columns) {
        if (qCol.column == "*") {
            if (qCol.table != null) {
                // Add all columns from specific table
                val prefix = "${qCol.table}."
                joinedRows.firstOrNull()?.keys?.filter { it.startsWith(prefix) }?.forEach {
                    resultColumns.add(qCol.alias ?: it)
                }
            } else {
                // Add all columns from all tables
                joinedRows.firstOrNull()?.keys?.forEach { resultColumns.add(it) }
            }
        } else {
            val fullName = if (qCol.table != null) "${qCol.table}.${qCol.column}" else qCol.column
            resultColumns.add(qCol.alias ?: fullName)
        }
    }

    // Extract rows based on selected columns
    val rows = joinedRows.map { joinedRow ->
        query.columns.flatMap { qCol ->
            if (qCol.column == "*") {
                if (qCol.table != null) {
                    val prefix = "${qCol.table}."
                    joinedRow.filterKeys { it.startsWith(prefix) }.values.toList()
                } else {
                    joinedRow.values.toList()
                }
            } else {
                val fullName = if (qCol.table != null) "${qCol.table}.${qCol.column}" else {
                    // Try to find the column in any table
                    joinedRow.keys.find { it.endsWith(".${qCol.column}") } ?: qCol.column
                }
                listOf(joinedRow[fullName])
            }
        }
    }

    // Apply ORDER BY
    val ordered = if (query.orderBy.isNotEmpty()) {
        applyOrderingToRows(rows, query.orderBy, resultColumns)
    } else {
        rows
    }

    // Apply LIMIT
    val limited = query.limit?.let { ordered.take(it.toInt()) } ?: ordered

    return QueryResult(resultColumns, limited)
}

private fun innerJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>,
    joinInfo: JoinInfo
): List<Map<String, Any?>> {
    val condition = joinInfo.condition ?: return crossJoin(leftRows, rightRows)

    val result = mutableListOf<Map<String, Any?>>()
    for (leftRow in leftRows) {
        val leftKey = "${condition.leftTable}.${condition.leftColumn}"
        val leftValue = leftRow[leftKey]

        for (rightRow in rightRows) {
            val rightKey = "${condition.rightTable}.${condition.rightColumn}"
            val rightValue = rightRow[rightKey]

            if (compareValues(leftValue, rightValue) == 0) {
                result.add(leftRow + rightRow)
            }
        }
    }
    return result
}

private fun leftJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>,
    joinInfo: JoinInfo
): List<Map<String, Any?>> {
    val condition = joinInfo.condition ?: return crossJoin(leftRows, rightRows)

    val rightNullRow = rightRows.firstOrNull()?.mapValues { null } ?: emptyMap()
    val result = mutableListOf<Map<String, Any?>>()

    for (leftRow in leftRows) {
        val leftKey = "${condition.leftTable}.${condition.leftColumn}"
        val leftValue = leftRow[leftKey]

        var matched = false
        for (rightRow in rightRows) {
            val rightKey = "${condition.rightTable}.${condition.rightColumn}"
            val rightValue = rightRow[rightKey]

            if (compareValues(leftValue, rightValue) == 0) {
                result.add(leftRow + rightRow)
                matched = true
            }
        }

        if (!matched) {
            result.add(leftRow + rightNullRow)
        }
    }
    return result
}

private fun rightJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>,
    joinInfo: JoinInfo
): List<Map<String, Any?>> {
    val condition = joinInfo.condition ?: return crossJoin(leftRows, rightRows)

    val leftNullRow = leftRows.firstOrNull()?.mapValues { null } ?: emptyMap()
    val result = mutableListOf<Map<String, Any?>>()

    for (rightRow in rightRows) {
        val rightKey = "${condition.rightTable}.${condition.rightColumn}"
        val rightValue = rightRow[rightKey]

        var matched = false
        for (leftRow in leftRows) {
            val leftKey = "${condition.leftTable}.${condition.leftColumn}"
            val leftValue = leftRow[leftKey]

            if (compareValues(leftValue, rightValue) == 0) {
                result.add(leftRow + rightRow)
                matched = true
            }
        }

        if (!matched) {
            result.add(leftNullRow + rightRow)
        }
    }
    return result
}

private fun fullJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>,
    joinInfo: JoinInfo
): List<Map<String, Any?>> {
    val condition = joinInfo.condition ?: return crossJoin(leftRows, rightRows)

    val leftNullRow = leftRows.firstOrNull()?.mapValues { null } ?: emptyMap()
    val rightNullRow = rightRows.firstOrNull()?.mapValues { null } ?: emptyMap()
    val result = mutableListOf<Map<String, Any?>>()
    val matchedRightIndices = mutableSetOf<Int>()

    for (leftRow in leftRows) {
        val leftKey = "${condition.leftTable}.${condition.leftColumn}"
        val leftValue = leftRow[leftKey]

        var matched = false
        for ((rightIdx, rightRow) in rightRows.withIndex()) {
            val rightKey = "${condition.rightTable}.${condition.rightColumn}"
            val rightValue = rightRow[rightKey]

            if (compareValues(leftValue, rightValue) == 0) {
                result.add(leftRow + rightRow)
                matchedRightIndices.add(rightIdx)
                matched = true
            }
        }

        if (!matched) {
            result.add(leftRow + rightNullRow)
        }
    }

    // Add unmatched right rows
    for ((rightIdx, rightRow) in rightRows.withIndex()) {
        if (rightIdx !in matchedRightIndices) {
            result.add(leftNullRow + rightRow)
        }
    }

    return result
}

private fun crossJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>
): List<Map<String, Any?>> {
    val result = mutableListOf<Map<String, Any?>>()
    for (leftRow in leftRows) {
        for (rightRow in rightRows) {
            result.add(leftRow + rightRow)
        }
    }
    return result
}

private fun naturalJoin(
    leftRows: List<Map<String, Any?>>,
    rightRows: List<Map<String, Any?>>,
    rightTable: String
): List<Map<String, Any?>> {
    // Find common column names
    val leftCols = leftRows.firstOrNull()?.keys?.map { it.substringAfter(".") }?.toSet() ?: emptySet()
    val rightCols = rightRows.firstOrNull()?.keys?.map { it.substringAfter(".") }?.toSet() ?: emptySet()
    val commonCols = leftCols.intersect(rightCols)

    if (commonCols.isEmpty()) {
        return crossJoin(leftRows, rightRows)
    }

    val result = mutableListOf<Map<String, Any?>>()
    for (leftRow in leftRows) {
        for (rightRow in rightRows) {
            val matches = commonCols.all { col ->
                val leftKey = leftRow.keys.find { it.endsWith(".$col") }
                val rightKey = rightRow.keys.find { it.endsWith(".$col") }
                leftKey != null && rightKey != null && compareValues(leftRow[leftKey], rightRow[rightKey]) == 0
            }
            if (matches) {
                result.add(leftRow + rightRow)
            }
        }
    }
    return result
}

    private fun applyOrderingToRows(
        rows: List<List<Any?>>,
        orderBy: List<OrderByItem>,
        columnNames: List<String>
    ): List<List<Any?>> {
        if (orderBy.isEmpty()) return rows

        return rows.sortedWith { a, b ->
            for (order in orderBy) {
                val colIdx = columnNames.indexOfFirst { it.equals(order.column, ignoreCase = true) || it.endsWith(".${order.column}") }
                if (colIdx >= 0 && colIdx < a.size && colIdx < b.size) {
                    val cmp = compareValues(a[colIdx], b[colIdx])
                    if (cmp != 0) {
                        return@sortedWith if (order.ascending) cmp else -cmp
                    }
                }
            }
            0
        }
    }

    companion object {
        // Execute in parallel when more than this many RPC calls
        private const val PARALLEL_THRESHOLD = 10
        // Number of concurrent threads for parallel execution
        private const val PARALLEL_THREADS = 20
    }
}

/**
 * Result of query execution.
 */
data class QueryResult(
    val columnNames: List<String>,
    val rows: List<List<Any?>>
)
