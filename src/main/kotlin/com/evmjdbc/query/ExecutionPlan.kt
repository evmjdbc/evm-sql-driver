package com.evmjdbc.query

import com.evmjdbc.rpc.RpcRequest

/**
 * Execution plan for a SQL query.
 * Contains the sequence of RPC calls needed and post-processing steps.
 */
data class ExecutionPlan(
    val tableName: String,
    val rpcCalls: List<RpcCall>,
    val selectExpressions: List<SelectExpression>,
    val postFilters: List<WhereCondition>,
    val orderBy: List<OrderByItem>,
    val limit: Long?,
    val distinct: Boolean = false
) {
    /**
     * Check if this is a single-row query (e.g., WHERE number = X).
     */
    fun isSingleRowQuery(): Boolean = rpcCalls.size == 1 && limit == 1L

    /**
     * Get output column names for the result set.
     */
    fun getOutputColumnNames(): List<String> = selectExpressions.map { it.getOutputName() }

    /**
     * Get all column references needed to evaluate expressions.
     */
    fun getRequiredColumns(): List<String> = selectExpressions.flatMap { it.getColumnReferences() }.distinct()
}

/**
 * Represents a single RPC call to execute.
 */
data class RpcCall(
    val method: String,
    val request: RpcRequest,
    val purpose: String
)

/**
 * Block range for queries.
 */
data class BlockRange(
    val from: Long,
    val to: Long
) {
    val size: Long get() = to - from + 1

    fun toList(): List<Long> = (from..to).toList()
}
