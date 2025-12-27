package com.evmjdbc.statement

import com.evmjdbc.connection.EvmConnection
import com.evmjdbc.exceptions.ErrorCode
import com.evmjdbc.exceptions.EvmSqlException
import com.evmjdbc.exceptions.RpcTranslationException
import com.evmjdbc.query.CompositeQuery
import com.evmjdbc.query.CteQuery
import com.evmjdbc.query.DualQuery
import com.evmjdbc.query.JoinedQuery
import com.evmjdbc.query.ParsedQuery
import com.evmjdbc.query.QueryExecutor
import com.evmjdbc.query.QueryParser
import com.evmjdbc.query.QueryPlanner
import com.evmjdbc.query.SubqueryQuery
import com.evmjdbc.resultset.EvmResultSet
import org.slf4j.LoggerFactory
import java.sql.*

/**
 * JDBC Statement implementation for EVM SQL queries.
 * Translates SQL SELECT statements to RPC calls.
 */
open class EvmStatement(
    protected val connection: EvmConnection
) : Statement {
    private val logger = LoggerFactory.getLogger(EvmStatement::class.java)

    // Query execution components
    private val queryParser = QueryParser()
    private val queryPlanner by lazy { QueryPlanner(connection.connectionProperties) }
    private val queryExecutor by lazy { QueryExecutor(connection.rpcClient, queryPlanner) }

    protected var closed = false
    protected var currentResultSet: ResultSet? = null
    protected var _maxRows = 0
    protected var _queryTimeout = 0
    protected var _fetchSize = 100
    protected var _fetchDirection = ResultSet.FETCH_FORWARD
    protected var _warnings: SQLWarning? = null
    protected var _resultSetType = ResultSet.TYPE_FORWARD_ONLY
    protected var _resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
    protected var _resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT
    protected var _poolable = false
    protected var _closeOnCompletion = false

    /**
     * Execute a SQL query and return a ResultSet.
     */
    override fun executeQuery(sql: String?): ResultSet {
        checkOpen()
        requireNotNull(sql) { "SQL string cannot be null" }

        // Close any previous result set
        currentResultSet?.close()

        logger.debug("Executing query: {}", sql.take(200))

        try {
            // Step 1: Parse SQL into QueryNode (ParsedQuery, CompositeQuery, or JoinedQuery)
            val queryNode = queryParser.parse(sql)

            val result = when (queryNode) {
                is DualQuery -> {
                    logger.debug("Executing dual query (SELECT without FROM)")
                    queryExecutor.executeQuery(queryNode)
                }
                is ParsedQuery -> {
                    logger.debug("Parsed query: table={}, expressions={}", queryNode.tableName, queryNode.selectExpressions.size)
                    // Step 2: Create execution plan
                    val executionPlan = queryPlanner.plan(queryNode)
                    logger.debug("Execution plan: {} RPC calls", executionPlan.rpcCalls.size)
                    // Step 3: Execute plan and get results
                    queryExecutor.execute(executionPlan)
                }
                is CompositeQuery -> {
                    logger.debug("Executing UNION query with {} sub-queries", queryNode.queries.size)
                    queryExecutor.executeQuery(queryNode)
                }
                is JoinedQuery -> {
                    logger.debug("Executing JOIN query with tables: {}", queryNode.getAllTables())
                    queryExecutor.executeQuery(queryNode)
                }
                is SubqueryQuery -> {
                    logger.debug("Executing subquery with alias: {}", queryNode.alias)
                    queryExecutor.executeQuery(queryNode)
                }
                is CteQuery -> {
                    logger.debug("Executing CTE query with {} CTEs", queryNode.ctes.size)
                    queryExecutor.executeQuery(queryNode)
                }
            }

            // Determine table name for metadata
            val tableName = when (queryNode) {
                is DualQuery -> "dual"
                is ParsedQuery -> queryNode.tableName
                is CompositeQuery -> "union_result"
                is JoinedQuery -> queryNode.getAllTables().joinToString("_")
                is SubqueryQuery -> queryNode.alias
                is CteQuery -> "cte_result"
            }

            // Step 4: Create and return ResultSet
            currentResultSet = EvmResultSet(
                statement = this,
                tableName = tableName,
                columnNames = result.columnNames,
                rows = result.rows
            )

            return currentResultSet!!

        } catch (e: RpcTranslationException) {
            logger.error("Query translation error: {}", e.message)
            throw EvmSqlException(ErrorCode.TRANSLATION_ERROR, e.message ?: "Query translation failed", e)
        } catch (e: SQLException) {
            logger.error("SQL error: {}", e.message)
            throw e
        } catch (e: Exception) {
            logger.error("Unexpected error executing query", e)
            throw EvmSqlException(ErrorCode.INTERNAL_ERROR, "Query execution failed: ${e.message}", e)
        }
    }

    /**
     * Execute an update statement - always throws for read-only driver.
     */
    override fun executeUpdate(sql: String?): Int {
        checkOpen()
        throw EvmSqlException(
            ErrorCode.READ_ONLY_VIOLATION,
            "INSERT, UPDATE, DELETE not supported. Use eth_sendRawTransaction via RPC directly."
        )
    }

    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        return executeUpdate(sql)
    }

    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        return executeUpdate(sql)
    }

    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        return executeUpdate(sql)
    }

    override fun execute(sql: String?): Boolean {
        checkOpen()
        requireNotNull(sql) { "SQL string cannot be null" }

        val normalizedSql = sql.trim().uppercase()

        // Check for write operations
        if (normalizedSql.startsWith("INSERT") ||
            normalizedSql.startsWith("UPDATE") ||
            normalizedSql.startsWith("DELETE") ||
            normalizedSql.startsWith("CREATE") ||
            normalizedSql.startsWith("DROP") ||
            normalizedSql.startsWith("ALTER")
        ) {
            throw EvmSqlException(
                ErrorCode.READ_ONLY_VIOLATION,
                "Write operations not supported. Use eth_sendRawTransaction via RPC directly."
            )
        }

        // For SELECT or WITH (CTE), execute query
        if (normalizedSql.startsWith("SELECT") || normalizedSql.startsWith("WITH")) {
            currentResultSet = executeQuery(sql)
            return true
        }

        throw EvmSqlException(ErrorCode.UNSUPPORTED_SYNTAX, "Unsupported SQL statement")
    }

    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        return execute(sql)
    }

    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        return execute(sql)
    }

    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        return execute(sql)
    }

    override fun getResultSet(): ResultSet? {
        checkOpen()
        return currentResultSet
    }

    override fun getUpdateCount(): Int {
        checkOpen()
        return -1 // No updates
    }

    override fun getMoreResults(): Boolean {
        checkOpen()
        currentResultSet?.close()
        currentResultSet = null
        return false
    }

    override fun getMoreResults(current: Int): Boolean {
        checkOpen()
        when (current) {
            Statement.CLOSE_CURRENT_RESULT -> currentResultSet?.close()
            Statement.CLOSE_ALL_RESULTS -> currentResultSet?.close()
        }
        currentResultSet = null
        return false
    }

    override fun getGeneratedKeys(): ResultSet {
        throw SQLFeatureNotSupportedException("Generated keys not supported")
    }

    // ========== Batch Operations (Not Supported) ==========

    override fun addBatch(sql: String?) {
        throw SQLFeatureNotSupportedException("Batch operations not supported for read-only driver")
    }

    override fun clearBatch() {
        // No-op
    }

    override fun executeBatch(): IntArray {
        throw SQLFeatureNotSupportedException("Batch operations not supported for read-only driver")
    }

    override fun executeLargeBatch(): LongArray {
        throw SQLFeatureNotSupportedException("Batch operations not supported for read-only driver")
    }

    override fun executeLargeUpdate(sql: String?): Long {
        throw EvmSqlException(ErrorCode.READ_ONLY_VIOLATION)
    }

    override fun executeLargeUpdate(sql: String?, autoGeneratedKeys: Int): Long {
        throw EvmSqlException(ErrorCode.READ_ONLY_VIOLATION)
    }

    override fun executeLargeUpdate(sql: String?, columnIndexes: IntArray?): Long {
        throw EvmSqlException(ErrorCode.READ_ONLY_VIOLATION)
    }

    override fun executeLargeUpdate(sql: String?, columnNames: Array<out String>?): Long {
        throw EvmSqlException(ErrorCode.READ_ONLY_VIOLATION)
    }

    override fun getLargeUpdateCount(): Long {
        checkOpen()
        return -1
    }

    override fun getLargeMaxRows(): Long {
        checkOpen()
        return _maxRows.toLong()
    }

    override fun setLargeMaxRows(max: Long) {
        checkOpen()
        _maxRows = max.toInt()
    }

    // ========== Settings ==========

    override fun setMaxRows(max: Int) {
        checkOpen()
        require(max >= 0) { "Max rows must be >= 0" }
        _maxRows = max
    }

    override fun getMaxRows(): Int {
        checkOpen()
        return _maxRows
    }

    override fun setQueryTimeout(seconds: Int) {
        checkOpen()
        require(seconds >= 0) { "Query timeout must be >= 0" }
        _queryTimeout = seconds
    }

    override fun getQueryTimeout(): Int {
        checkOpen()
        return _queryTimeout
    }

    override fun setFetchSize(rows: Int) {
        checkOpen()
        require(rows >= 0) { "Fetch size must be >= 0" }
        _fetchSize = rows
    }

    override fun getFetchSize(): Int {
        checkOpen()
        return _fetchSize
    }

    override fun setFetchDirection(direction: Int) {
        checkOpen()
        if (direction != ResultSet.FETCH_FORWARD &&
            direction != ResultSet.FETCH_REVERSE &&
            direction != ResultSet.FETCH_UNKNOWN
        ) {
            throw SQLException("Invalid fetch direction: $direction")
        }
        _fetchDirection = direction
    }

    override fun getFetchDirection(): Int {
        checkOpen()
        return _fetchDirection
    }

    override fun setMaxFieldSize(max: Int) {
        checkOpen()
        // Ignored
    }

    override fun getMaxFieldSize(): Int {
        checkOpen()
        return 0
    }

    override fun setEscapeProcessing(enable: Boolean) {
        checkOpen()
        // Ignored
    }

    override fun setCursorName(name: String?) {
        throw SQLFeatureNotSupportedException("Named cursors not supported")
    }

    override fun setPoolable(poolable: Boolean) {
        checkOpen()
        _poolable = poolable
    }

    override fun isPoolable(): Boolean {
        checkOpen()
        return _poolable
    }

    override fun closeOnCompletion() {
        checkOpen()
        _closeOnCompletion = true
    }

    override fun isCloseOnCompletion(): Boolean {
        checkOpen()
        return _closeOnCompletion
    }

    override fun getResultSetType(): Int {
        checkOpen()
        return _resultSetType
    }

    override fun getResultSetConcurrency(): Int {
        checkOpen()
        return _resultSetConcurrency
    }

    override fun getResultSetHoldability(): Int {
        checkOpen()
        return _resultSetHoldability
    }

    // ========== Warnings ==========

    override fun getWarnings(): SQLWarning? {
        checkOpen()
        return _warnings
    }

    override fun clearWarnings() {
        checkOpen()
        _warnings = null
    }

    protected fun addWarning(warning: SQLWarning) {
        if (_warnings == null) {
            _warnings = warning
        } else {
            _warnings?.nextWarning = warning
        }
    }

    // ========== Close ==========

    override fun close() {
        if (!closed) {
            currentResultSet?.close()
            currentResultSet = null
            closed = true
        }
    }

    override fun isClosed(): Boolean = closed

    override fun cancel() {
        // Not supported - operations are synchronous
    }

    // ========== Connection ==========

    override fun getConnection(): Connection {
        checkOpen()
        return connection
    }

    // ========== Wrapper ==========

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        if (iface != null && iface.isInstance(this)) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLException("Cannot unwrap to ${iface?.name}")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        return iface?.isInstance(this) == true
    }

    // ========== Helpers ==========

    protected fun checkOpen() {
        if (closed) {
            throw SQLException("Statement is closed")
        }
        if (connection.isClosed) {
            throw SQLException("Connection is closed")
        }
    }
}
