package com.evmjdbc.query

import com.evmjdbc.exceptions.RpcTranslationException
import net.sf.jsqlparser.expression.*
import net.sf.jsqlparser.expression.operators.arithmetic.*
import net.sf.jsqlparser.expression.operators.conditional.AndExpression
import net.sf.jsqlparser.expression.operators.conditional.OrExpression
import net.sf.jsqlparser.expression.operators.relational.*
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema.Column
import net.sf.jsqlparser.schema.Table
import net.sf.jsqlparser.statement.select.*
import net.sf.jsqlparser.statement.select.ParenthesedSelect
import net.sf.jsqlparser.statement.select.WithItem

/**
 * Parses SQL SELECT statements using JSqlParser.
 * Supports simple queries, UNION/UNION ALL, and JOINs.
 */
class QueryParser {

    // Store CTE definitions during parsing (for resolving CTE references)
    private var currentCtes: Map<String, QueryNode> = emptyMap()

    /**
     * Parse a SQL statement and return a ParsedQuery or CompositeQuery.
     */
    fun parse(sql: String): QueryNode {
        val statement = try {
            CCJSqlParserUtil.parse(sql)
        } catch (e: Exception) {
            throw RpcTranslationException("Failed to parse SQL: ${e.message}")
        }

        if (statement !is Select) {
            throw RpcTranslationException("Only SELECT statements are supported")
        }

        // Check for CTEs (WITH clause)
        val withItems = statement.withItemsList
        if (!withItems.isNullOrEmpty()) {
            return parseCteQuery(statement, withItems)
        }

        // Use selectBody to determine the type - plainSelect throws ClassCastException for UNION
        @Suppress("DEPRECATION")
        val selectBody = statement.selectBody
        return when (selectBody) {
            is SetOperationList -> parseSetOperationList(selectBody)
            is PlainSelect -> parsePlainSelect(selectBody)
            else -> throw RpcTranslationException("Unsupported SELECT type: ${selectBody?.javaClass?.simpleName ?: "null"}")
        }
    }

    /**
     * Parse a query with CTEs (WITH clause).
     */
    private fun parseCteQuery(statement: Select, withItems: List<WithItem>): CteQuery {
        // Parse each CTE definition
        val cteDefinitions = mutableListOf<CteDefinition>()
        val cteMap = mutableMapOf<String, QueryNode>()

        for (withItem in withItems) {
            val cteName = withItem.alias?.name?.lowercase()
                ?: throw RpcTranslationException("CTE must have a name")

            // Temporarily set current CTEs for nested references
            currentCtes = cteMap.toMap()

            // Parse the CTE's subquery - handle different wrapper types
            val cteQuery = parseCteSelectBody(withItem, cteName)

            cteDefinitions.add(CteDefinition(cteName, cteQuery))
            cteMap[cteName] = cteQuery
        }

        // Set CTEs for main query parsing
        currentCtes = cteMap.toMap()

        // Parse the main query
        @Suppress("DEPRECATION")
        val mainSelectBody = statement.selectBody
        val mainQuery = when (mainSelectBody) {
            is PlainSelect -> parsePlainSelect(mainSelectBody)
            is SetOperationList -> parseSetOperationList(mainSelectBody)
            else -> throw RpcTranslationException("Unsupported main query type: ${mainSelectBody?.javaClass?.simpleName}")
        }

        // Clear CTEs after parsing
        currentCtes = emptyMap()

        return CteQuery(cteDefinitions, mainQuery)
    }

    /**
     * Parse the SELECT body of a CTE, handling various wrapper types.
     */
    private fun parseCteSelectBody(withItem: WithItem, cteName: String): QueryNode {
        // Try to get select body directly
        @Suppress("DEPRECATION")
        val select = withItem.select
            ?: throw RpcTranslationException("CTE '$cteName' must have a SELECT body")

        // Handle ParenthesedSelect wrapper
        if (select is ParenthesedSelect) {
            @Suppress("DEPRECATION")
            val innerBody = select.select?.selectBody
            return when (innerBody) {
                is PlainSelect -> parsePlainSelect(innerBody)
                is SetOperationList -> parseSetOperationList(innerBody)
                else -> throw RpcTranslationException("Unsupported CTE query type in parentheses: ${innerBody?.javaClass?.simpleName}")
            }
        }

        // Handle direct select body
        @Suppress("DEPRECATION")
        val selectBody = select.selectBody
        return when (selectBody) {
            is PlainSelect -> parsePlainSelect(selectBody)
            is SetOperationList -> parseSetOperationList(selectBody)
            is ParenthesedSelect -> {
                // Nested ParenthesedSelect
                @Suppress("DEPRECATION")
                val innerBody = selectBody.select?.selectBody
                when (innerBody) {
                    is PlainSelect -> parsePlainSelect(innerBody)
                    is SetOperationList -> parseSetOperationList(innerBody)
                    else -> throw RpcTranslationException("Unsupported nested CTE query type: ${innerBody?.javaClass?.simpleName}")
                }
            }
            else -> throw RpcTranslationException("Unsupported CTE query type: ${selectBody?.javaClass?.simpleName}")
        }
    }

    /**
     * Parse a SQL statement and return a simple ParsedQuery (for backwards compatibility).
     * Throws if the query is a UNION or complex query.
     */
    fun parseSimple(sql: String): ParsedQuery {
        val result = parse(sql)
        return when (result) {
            is ParsedQuery -> result
            is DualQuery -> throw RpcTranslationException("Expected table query but got dual query (SELECT without FROM)")
            is CompositeQuery -> throw RpcTranslationException("Expected simple query but got UNION/composite query")
            is JoinedQuery -> throw RpcTranslationException("Expected simple query but got JOIN query")
            is SubqueryQuery -> throw RpcTranslationException("Expected simple query but got subquery")
            is CteQuery -> throw RpcTranslationException("Expected simple query but got CTE query")
        }
    }

    private fun parseSetOperationList(setOp: SetOperationList): CompositeQuery {
        val selects = setOp.selects
        val operations = setOp.operations

        if (selects.isEmpty()) {
            throw RpcTranslationException("Empty UNION query")
        }

        // Parse all SELECT statements
        val queries = selects.map { select ->
            when (select) {
                is PlainSelect -> parsePlainSelect(select)
                is SetOperationList -> parseSetOperationList(select)
                else -> throw RpcTranslationException("Unsupported SELECT in UNION: ${select?.javaClass?.simpleName ?: "null"}")
            }
        }

        // Determine operation types (UNION vs UNION ALL)
        val ops = operations.map { op ->
            when (op) {
                is UnionOp -> if (op.isAll) SetOperation.UNION_ALL else SetOperation.UNION
                is IntersectOp -> SetOperation.INTERSECT
                is ExceptOp -> SetOperation.EXCEPT
                is MinusOp -> SetOperation.EXCEPT
                else -> throw RpcTranslationException("Unsupported set operation: ${op.javaClass.simpleName}")
            }
        }

        // Parse global ORDER BY and LIMIT if present
        val orderBy = setOp.orderByElements?.map { element ->
            OrderByItem(
                column = (element.expression as? Column)?.columnName?.lowercase()
                    ?: throw RpcTranslationException("Only column names supported in ORDER BY"),
                ascending = element.isAsc
            )
        } ?: emptyList()

        val limit = setOp.limit?.rowCount?.let { extractLongValue(it) }

        return CompositeQuery(
            queries = queries,
            operations = ops,
            orderBy = orderBy,
            limit = limit
        )
    }

    private fun parsePlainSelect(plainSelect: PlainSelect): QueryNode {
        // Check for JOINs first
        val joins = plainSelect.joins
        if (!joins.isNullOrEmpty()) {
            return parseJoinedQuery(plainSelect)
        }

        // Handle queries without FROM clause (e.g., SELECT 1, SELECT 'test')
        val fromItem = plainSelect.fromItem
        if (fromItem == null) {
            // This is a constant/literal query - return a special "dual" table query
            return parseDualQuery(plainSelect)
        }

        // Parse the FROM item - could be table, subquery, or CTE reference
        val fromSource = parseFromItemExtended(fromItem)

        // Parse common elements
        val tableAlias = when (fromSource) {
            is FromSource.TableSource -> fromSource.alias
            is FromSource.SubquerySource -> fromSource.alias
            is FromSource.CteReference -> fromSource.alias
        }

        // Parse SELECT expressions
        val selectExpressions = parseSelectItems(plainSelect.selectItems, tableAlias)

        // Parse WHERE clause
        val conditions = plainSelect.where?.let { parseWhere(it, tableAlias) } ?: emptyList()

        // Parse LIMIT
        val limit = plainSelect.limit?.rowCount?.let { extractLongValue(it) }

        // Parse ORDER BY
        val orderBy = plainSelect.orderByElements?.map { element ->
            OrderByItem(
                column = extractColumnName(element.expression, tableAlias),
                ascending = element.isAsc
            )
        } ?: emptyList()

        // Check for DISTINCT
        val distinct = plainSelect.distinct != null

        // Return appropriate query type based on FROM source
        return when (fromSource) {
            is FromSource.TableSource -> ParsedQuery(
                tableName = fromSource.tableName,
                selectExpressions = selectExpressions,
                conditions = conditions,
                limit = limit,
                orderBy = orderBy,
                distinct = distinct
            )
            is FromSource.SubquerySource -> SubqueryQuery(
                subquery = fromSource.query,
                alias = fromSource.alias,
                selectExpressions = selectExpressions,
                conditions = conditions,
                limit = limit,
                orderBy = orderBy,
                distinct = distinct
            )
            is FromSource.CteReference -> {
                // For CTE references, we create a SubqueryQuery that references the CTE
                val cteQuery = currentCtes[fromSource.cteName]
                    ?: throw RpcTranslationException("Unknown CTE: ${fromSource.cteName}")
                SubqueryQuery(
                    subquery = cteQuery,
                    alias = fromSource.alias ?: fromSource.cteName,
                    selectExpressions = selectExpressions,
                    conditions = conditions,
                    limit = limit,
                    orderBy = orderBy,
                    distinct = distinct
                )
            }
        }
    }

    /**
     * Parse a query without FROM clause (e.g., SELECT 1, SELECT 'hello').
     * Returns a DualQuery that represents constant expressions.
     */
    private fun parseDualQuery(plainSelect: PlainSelect): DualQuery {
        val values = mutableListOf<Pair<String, Any?>>()

        for (item in plainSelect.selectItems) {
            val expr = item.expression
            val alias = item.alias?.name

            val (name, value) = when (expr) {
                is LongValue -> Pair(alias ?: expr.value.toString(), expr.value)
                is DoubleValue -> Pair(alias ?: expr.value.toString(), expr.value)
                is StringValue -> Pair(alias ?: "'${expr.value}'", expr.value)
                is NullValue -> Pair(alias ?: "NULL", null)
                is Column -> {
                    // Handle boolean literals
                    when (expr.columnName.lowercase()) {
                        "true" -> Pair(alias ?: "true", true)
                        "false" -> Pair(alias ?: "false", false)
                        else -> Pair(alias ?: expr.columnName, expr.columnName)
                    }
                }
                is net.sf.jsqlparser.expression.Function -> {
                    // Handle functions like NOW(), CURRENT_TIMESTAMP, etc.
                    Pair(alias ?: expr.toString(), expr.toString())
                }
                else -> Pair(alias ?: expr.toString(), expr.toString())
            }
            values.add(Pair(name, value))
        }

        return DualQuery(values)
    }

    private fun parseJoinedQuery(plainSelect: PlainSelect): JoinedQuery {
        // Parse the main FROM table
        val fromItem = plainSelect.fromItem
            ?: throw RpcTranslationException("FROM clause is required")

        val (leftTable, leftAlias) = parseFromItem(fromItem)

        // Build table alias map
        val tableAliases = mutableMapOf<String, String>()
        if (leftAlias != null) {
            tableAliases[leftAlias] = leftTable
        }
        tableAliases[leftTable] = leftTable

        // Parse all joins
        val joinInfos = mutableListOf<JoinInfo>()
        for (join in plainSelect.joins ?: emptyList()) {
            val (rightTable, rightAlias) = parseFromItem(join.fromItem)

            if (rightAlias != null) {
                tableAliases[rightAlias] = rightTable
            }
            tableAliases[rightTable] = rightTable

            val joinType = when {
                join.isLeft -> JoinType.LEFT
                join.isRight -> JoinType.RIGHT
                join.isFull -> JoinType.FULL
                join.isCross -> JoinType.CROSS
                join.isNatural -> JoinType.NATURAL
                else -> JoinType.INNER
            }

            // Parse ON clause
            val onExpression = join.onExpressions?.firstOrNull()
            val joinCondition = if (onExpression != null) {
                parseJoinCondition(onExpression, tableAliases)
            } else {
                null
            }

            joinInfos.add(JoinInfo(
                rightTable = rightTable,
                rightAlias = rightAlias,
                joinType = joinType,
                condition = joinCondition
            ))
        }

        // Parse SELECT columns with table qualifiers
        val columns = parseSelectItemsWithQualifiers(plainSelect.selectItems, tableAliases)

        // Parse WHERE clause
        val conditions = plainSelect.where?.let { parseWhereWithQualifiers(it, tableAliases) } ?: emptyList()

        // Parse LIMIT
        val limit = plainSelect.limit?.rowCount?.let { extractLongValue(it) }

        // Parse ORDER BY
        val orderBy = plainSelect.orderByElements?.map { element ->
            val col = element.expression
            val colName = when (col) {
                is Column -> {
                    val table = col.table?.name?.lowercase()
                    val name = col.columnName.lowercase()
                    if (table != null) "$table.$name" else name
                }
                else -> col.toString().lowercase()
            }
            OrderByItem(column = colName, ascending = element.isAsc)
        } ?: emptyList()

        return JoinedQuery(
            leftTable = leftTable,
            leftAlias = leftAlias,
            joins = joinInfos,
            columns = columns,
            conditions = conditions,
            limit = limit,
            orderBy = orderBy,
            tableAliases = tableAliases
        )
    }

    private fun parseJoinCondition(expr: Expression, tableAliases: Map<String, String>): JoinCondition {
        return when (expr) {
            is EqualsTo -> {
                val left = expr.leftExpression
                val right = expr.rightExpression

                if (left is Column && right is Column) {
                    val leftTable = resolveTableName(left.table?.name, tableAliases)
                    val rightTable = resolveTableName(right.table?.name, tableAliases)

                    JoinCondition(
                        leftTable = leftTable,
                        leftColumn = left.columnName.lowercase(),
                        rightTable = rightTable,
                        rightColumn = right.columnName.lowercase()
                    )
                } else {
                    throw RpcTranslationException("JOIN ON clause must compare columns from both tables")
                }
            }
            else -> throw RpcTranslationException("Only equality conditions supported in JOIN ON clause")
        }
    }

    private fun resolveTableName(alias: String?, tableAliases: Map<String, String>): String {
        if (alias == null) {
            // Try to infer from the first table
            return tableAliases.values.firstOrNull() ?: throw RpcTranslationException("Cannot resolve table name")
        }
        return tableAliases[alias.lowercase()] ?: alias.lowercase()
    }

    /**
     * Result of parsing a FROM item - can be either a table name or a subquery.
     */
    sealed class FromSource {
        data class TableSource(val tableName: String, val alias: String?) : FromSource()
        data class SubquerySource(val query: QueryNode, val alias: String) : FromSource()
        data class CteReference(val cteName: String, val alias: String?) : FromSource()
    }

    private fun parseFromItem(fromItem: FromItem): Pair<String, String?> {
        return when (val source = parseFromItemExtended(fromItem)) {
            is FromSource.TableSource -> Pair(source.tableName, source.alias)
            is FromSource.CteReference -> Pair(source.cteName, source.alias)
            is FromSource.SubquerySource -> throw RpcTranslationException(
                "Subquery in FROM must be handled by parsePlainSelect directly"
            )
        }
    }

    private fun parseFromItemExtended(fromItem: FromItem): FromSource {
        return when (fromItem) {
            is Table -> {
                val tableName = fromItem.name.lowercase()
                val alias = fromItem.alias?.name?.lowercase()

                // Check if this references a CTE
                if (currentCtes.containsKey(tableName)) {
                    FromSource.CteReference(tableName, alias)
                } else {
                    FromSource.TableSource(tableName, alias)
                }
            }
            is ParenthesedSelect -> {
                val alias = fromItem.alias?.name?.lowercase()
                    ?: throw RpcTranslationException("Subquery in FROM clause must have an alias")

                @Suppress("DEPRECATION")
                val selectBody = fromItem.select?.selectBody
                    ?: throw RpcTranslationException("Invalid subquery in FROM clause")

                val subquery = when (selectBody) {
                    is PlainSelect -> parsePlainSelect(selectBody)
                    is SetOperationList -> parseSetOperationList(selectBody)
                    else -> throw RpcTranslationException("Unsupported subquery type: ${selectBody.javaClass.simpleName}")
                }

                FromSource.SubquerySource(subquery, alias)
            }
            else -> throw RpcTranslationException("Unsupported FROM item: ${fromItem.javaClass.simpleName}")
        }
    }

    private fun parseSelectItems(items: List<SelectItem<*>>, tableAlias: String?): List<SelectExpression> {
        return items.flatMap { item ->
            when (item) {
                is AllColumns -> listOf(SelectExpression.Column("*"))
                is AllTableColumns -> listOf(SelectExpression.Column("*"))
                else -> {
                    val expr = item.expression
                    val alias = item.alias?.name
                    listOf(parseExpression(expr, alias))
                }
            }
        }
    }

    /**
     * Parse an expression into a SelectExpression.
     */
    private fun parseExpression(expr: Expression, alias: String?): SelectExpression {
        return when (expr) {
            is AllColumns -> SelectExpression.Column("*", alias)
            is AllTableColumns -> SelectExpression.Column("*", alias)
            is Column -> {
                val colName = expr.columnName
                if (colName == "*") SelectExpression.Column("*", alias)
                else SelectExpression.Column(colName.lowercase(), alias)
            }
            is LongValue -> SelectExpression.Literal(expr.value, alias)
            is DoubleValue -> SelectExpression.Literal(expr.value, alias)
            is StringValue -> SelectExpression.Literal(expr.value, alias)
            is NullValue -> SelectExpression.Literal(null, alias)
            is Addition -> SelectExpression.Arithmetic(
                left = parseExpression(expr.leftExpression, null),
                operator = ArithmeticOperator.ADD,
                right = parseExpression(expr.rightExpression, null),
                alias = alias,
                originalExpression = expr.toString()
            )
            is Subtraction -> SelectExpression.Arithmetic(
                left = parseExpression(expr.leftExpression, null),
                operator = ArithmeticOperator.SUBTRACT,
                right = parseExpression(expr.rightExpression, null),
                alias = alias,
                originalExpression = expr.toString()
            )
            is Multiplication -> SelectExpression.Arithmetic(
                left = parseExpression(expr.leftExpression, null),
                operator = ArithmeticOperator.MULTIPLY,
                right = parseExpression(expr.rightExpression, null),
                alias = alias,
                originalExpression = expr.toString()
            )
            is Division -> SelectExpression.Arithmetic(
                left = parseExpression(expr.leftExpression, null),
                operator = ArithmeticOperator.DIVIDE,
                right = parseExpression(expr.rightExpression, null),
                alias = alias,
                originalExpression = expr.toString()
            )
            is Modulo -> SelectExpression.Arithmetic(
                left = parseExpression(expr.leftExpression, null),
                operator = ArithmeticOperator.MODULO,
                right = parseExpression(expr.rightExpression, null),
                alias = alias,
                originalExpression = expr.toString()
            )
            is Parenthesis -> parseExpression(expr.expression, alias)
            is SignedExpression -> {
                val inner = parseExpression(expr.expression, null)
                if (expr.sign == '-') {
                    SelectExpression.Arithmetic(
                        left = SelectExpression.Literal(0L, null),
                        operator = ArithmeticOperator.SUBTRACT,
                        right = inner,
                        alias = alias,
                        originalExpression = expr.toString()
                    )
                } else {
                    if (alias != null && inner is SelectExpression.Column) {
                        SelectExpression.Column(inner.name, alias)
                    } else if (alias != null && inner is SelectExpression.Literal) {
                        SelectExpression.Literal(inner.value, alias)
                    } else {
                        inner
                    }
                }
            }
            is net.sf.jsqlparser.expression.Function -> {
                val args = expr.parameters?.expressions?.map { parseExpression(it, null) } ?: emptyList()
                SelectExpression.Function(expr.name.lowercase(), args, alias)
            }
            else -> {
                // Fallback: treat as a literal string representation
                SelectExpression.Literal(expr.toString(), alias)
            }
        }
    }

    private fun parseSelectItemsWithQualifiers(items: List<SelectItem<*>>, tableAliases: Map<String, String>): List<QualifiedColumn> {
        return items.flatMap { item ->
            when (item) {
                is AllColumns -> listOf(QualifiedColumn(null, "*", item.alias?.name))
                is AllTableColumns -> {
                    val tableName = item.table?.name?.lowercase()
                    listOf(QualifiedColumn(tableAliases[tableName] ?: tableName, "*", null))
                }
                else -> {
                    val expr = item.expression
                    val alias = item.alias?.name
                    when (expr) {
                        is AllColumns -> listOf(QualifiedColumn(null, "*", alias))
                        is AllTableColumns -> {
                            val tableName = expr.table?.name?.lowercase()
                            listOf(QualifiedColumn(tableAliases[tableName] ?: tableName, "*", alias))
                        }
                        is Column -> {
                            val tableName = expr.table?.name?.lowercase()
                            val colName = expr.columnName
                            if (colName == "*") {
                                listOf(QualifiedColumn(tableAliases[tableName] ?: tableName, "*", alias))
                            } else {
                                listOf(QualifiedColumn(tableAliases[tableName] ?: tableName, colName.lowercase(), alias))
                            }
                        }
                        is net.sf.jsqlparser.expression.Function -> {
                            listOf(QualifiedColumn(null, alias ?: expr.name.lowercase(), alias))
                        }
                        else -> {
                            val str = expr.toString()
                            if (str.endsWith(".*")) {
                                listOf(QualifiedColumn(null, "*", alias))
                            } else {
                                listOf(QualifiedColumn(null, alias ?: str, alias))
                            }
                        }
                    }
                }
            }
        }
    }

    private fun extractColumnName(expr: Expression, tableAlias: String?): String {
        return when (expr) {
            is Column -> expr.columnName.lowercase()
            else -> throw RpcTranslationException("Only column names supported in ORDER BY")
        }
    }

    private fun parseWhere(expression: Expression, tableAlias: String?): List<WhereCondition> {
        return when (expression) {
            is AndExpression -> {
                parseWhere(expression.leftExpression, tableAlias) + parseWhere(expression.rightExpression, tableAlias)
            }
            is OrExpression -> {
                throw RpcTranslationException("OR conditions not supported", clause = "WHERE")
            }
            is EqualsTo -> listOf(parseComparisonExpression(expression, ComparisonOperator.EQUALS, tableAlias))
            is GreaterThan -> listOf(parseComparisonExpression(expression, ComparisonOperator.GREATER_THAN, tableAlias))
            is GreaterThanEquals -> listOf(parseComparisonExpression(expression, ComparisonOperator.GREATER_THAN_EQUALS, tableAlias))
            is MinorThan -> listOf(parseComparisonExpression(expression, ComparisonOperator.LESS_THAN, tableAlias))
            is MinorThanEquals -> listOf(parseComparisonExpression(expression, ComparisonOperator.LESS_THAN_EQUALS, tableAlias))
            is Between -> parseBetween(expression, tableAlias)
            is InExpression -> parseIn(expression, tableAlias)
            is IsNullExpression -> listOf(parseIsNull(expression, tableAlias))
            is Parenthesis -> parseWhere(expression.expression, tableAlias)
            else -> throw RpcTranslationException("Unsupported WHERE expression: ${expression.javaClass.simpleName}")
        }
    }

    private fun parseWhereWithQualifiers(expression: Expression, tableAliases: Map<String, String>): List<QualifiedWhereCondition> {
        return when (expression) {
            is AndExpression -> {
                parseWhereWithQualifiers(expression.leftExpression, tableAliases) +
                        parseWhereWithQualifiers(expression.rightExpression, tableAliases)
            }
            is OrExpression -> {
                throw RpcTranslationException("OR conditions not supported", clause = "WHERE")
            }
            is EqualsTo -> listOf(parseQualifiedComparison(expression, ComparisonOperator.EQUALS, tableAliases))
            is GreaterThan -> listOf(parseQualifiedComparison(expression, ComparisonOperator.GREATER_THAN, tableAliases))
            is GreaterThanEquals -> listOf(parseQualifiedComparison(expression, ComparisonOperator.GREATER_THAN_EQUALS, tableAliases))
            is MinorThan -> listOf(parseQualifiedComparison(expression, ComparisonOperator.LESS_THAN, tableAliases))
            is MinorThanEquals -> listOf(parseQualifiedComparison(expression, ComparisonOperator.LESS_THAN_EQUALS, tableAliases))
            is Between -> parseQualifiedBetween(expression, tableAliases)
            is InExpression -> parseQualifiedIn(expression, tableAliases)
            is IsNullExpression -> listOf(parseQualifiedIsNull(expression, tableAliases))
            is Parenthesis -> parseWhereWithQualifiers(expression.expression, tableAliases)
            else -> throw RpcTranslationException("Unsupported WHERE expression: ${expression.javaClass.simpleName}")
        }
    }

    private fun parseQualifiedComparison(expr: BinaryExpression, op: ComparisonOperator, tableAliases: Map<String, String>): QualifiedWhereCondition {
        val left = expr.leftExpression
        val right = expr.rightExpression

        return when (left) {
            is Column -> {
                val tableName = left.table?.name?.lowercase()
                val resolvedTable = if (tableName != null) tableAliases[tableName] ?: tableName else null
                QualifiedWhereCondition(resolvedTable, left.columnName.lowercase(), op, extractValue(right))
            }
            else -> throw RpcTranslationException("Left side of comparison must be a column")
        }
    }

    private fun parseQualifiedBetween(expr: Between, tableAliases: Map<String, String>): List<QualifiedWhereCondition> {
        val col = expr.leftExpression
        return when (col) {
            is Column -> {
                val tableName = col.table?.name?.lowercase()
                val resolvedTable = if (tableName != null) tableAliases[tableName] ?: tableName else null
                val column = col.columnName.lowercase()
                val start = extractValue(expr.betweenExpressionStart)
                val end = extractValue(expr.betweenExpressionEnd)
                listOf(
                    QualifiedWhereCondition(resolvedTable, column, ComparisonOperator.GREATER_THAN_EQUALS, start),
                    QualifiedWhereCondition(resolvedTable, column, ComparisonOperator.LESS_THAN_EQUALS, end)
                )
            }
            else -> throw RpcTranslationException("BETWEEN requires a column on the left side")
        }
    }

    private fun parseQualifiedIn(expr: InExpression, tableAliases: Map<String, String>): List<QualifiedWhereCondition> {
        val col = expr.leftExpression
        return when (col) {
            is Column -> {
                val tableName = col.table?.name?.lowercase()
                val resolvedTable = if (tableName != null) tableAliases[tableName] ?: tableName else null
                val rightExpression = expr.rightExpression
                val values = when (rightExpression) {
                    is ParenthesedExpressionList<*> -> rightExpression.map { extractValue(it as Expression) }
                    is ExpressionList<*> -> rightExpression.map { extractValue(it as Expression) }
                    else -> throw RpcTranslationException("IN clause requires a list of values")
                }
                listOf(QualifiedWhereCondition(resolvedTable, col.columnName.lowercase(), ComparisonOperator.IN, values))
            }
            else -> throw RpcTranslationException("IN requires a column on the left side")
        }
    }

    private fun parseQualifiedIsNull(expr: IsNullExpression, tableAliases: Map<String, String>): QualifiedWhereCondition {
        val col = expr.leftExpression
        return when (col) {
            is Column -> {
                val tableName = col.table?.name?.lowercase()
                val resolvedTable = if (tableName != null) tableAliases[tableName] ?: tableName else null
                val op = if (expr.isNot) ComparisonOperator.IS_NOT_NULL else ComparisonOperator.IS_NULL
                QualifiedWhereCondition(resolvedTable, col.columnName.lowercase(), op, null)
            }
            else -> throw RpcTranslationException("IS NULL requires a column")
        }
    }

    private fun parseComparisonExpression(expr: BinaryExpression, op: ComparisonOperator, tableAlias: String?): WhereCondition {
        val left = expr.leftExpression
        val right = expr.rightExpression

        val column = when (left) {
            is Column -> left.columnName.lowercase()
            else -> throw RpcTranslationException("Left side of comparison must be a column")
        }

        val value = extractValue(right)

        return WhereCondition(column, op, value)
    }

    private fun parseBetween(expr: Between, tableAlias: String?): List<WhereCondition> {
        val column = when (val col = expr.leftExpression) {
            is Column -> col.columnName.lowercase()
            else -> throw RpcTranslationException("BETWEEN requires a column on the left side")
        }

        val start = extractValue(expr.betweenExpressionStart)
        val end = extractValue(expr.betweenExpressionEnd)

        return listOf(
            WhereCondition(column, ComparisonOperator.GREATER_THAN_EQUALS, start),
            WhereCondition(column, ComparisonOperator.LESS_THAN_EQUALS, end)
        )
    }

    private fun parseIn(expr: InExpression, tableAlias: String?): List<WhereCondition> {
        val column = when (val col = expr.leftExpression) {
            is Column -> col.columnName.lowercase()
            else -> throw RpcTranslationException("IN requires a column on the left side")
        }

        val rightExpression = expr.rightExpression
        val values = when (rightExpression) {
            is ParenthesedExpressionList<*> -> rightExpression.map { extractValue(it as Expression) }
            is ExpressionList<*> -> rightExpression.map { extractValue(it as Expression) }
            else -> throw RpcTranslationException("IN clause requires a list of values")
        }

        return listOf(WhereCondition(column, ComparisonOperator.IN, values))
    }

    private fun parseIsNull(expr: IsNullExpression, tableAlias: String?): WhereCondition {
        val column = when (val col = expr.leftExpression) {
            is Column -> col.columnName.lowercase()
            else -> throw RpcTranslationException("IS NULL requires a column")
        }

        val op = if (expr.isNot) ComparisonOperator.IS_NOT_NULL else ComparisonOperator.IS_NULL
        return WhereCondition(column, op, null)
    }

    private fun extractValue(expr: Expression): Any {
        return when (expr) {
            is LongValue -> expr.value
            is DoubleValue -> expr.value
            is StringValue -> expr.value
            is HexValue -> expr.value
            is NullValue -> throw RpcTranslationException("NULL values in comparisons not supported")
            is SignedExpression -> {
                val value = extractValue(expr.expression)
                if (expr.sign == '-') {
                    when (value) {
                        is Long -> -value
                        is Double -> -value
                        else -> value
                    }
                } else value
            }
            is Column -> {
                when (expr.columnName.lowercase()) {
                    "true" -> true
                    "false" -> false
                    else -> throw RpcTranslationException("Unsupported value type: Column (${expr.columnName})")
                }
            }
            else -> throw RpcTranslationException("Unsupported value type: ${expr.javaClass.simpleName}")
        }
    }

    private fun extractLongValue(expr: Expression): Long? {
        return when (expr) {
            is LongValue -> expr.value
            else -> null
        }
    }
}

/**
 * Base sealed interface for query nodes.
 */
sealed interface QueryNode

/**
 * Parsed SQL query representation.
 */
data class ParsedQuery(
    val tableName: String,
    val selectExpressions: List<SelectExpression>,
    val conditions: List<WhereCondition>,
    val limit: Long?,
    val orderBy: List<OrderByItem>,
    val distinct: Boolean = false
) : QueryNode {
    fun getCondition(columnName: String): WhereCondition? =
        conditions.find { it.column == columnName.lowercase() }

    fun getConditions(columnName: String): List<WhereCondition> =
        conditions.filter { it.column == columnName.lowercase() }

    fun isSelectAll(): Boolean = selectExpressions.any {
        it is SelectExpression.Column && it.name == "*"
    }

    /**
     * Get all column names referenced in expressions (for backwards compatibility).
     */
    fun getReferencedColumns(): List<String> = selectExpressions.flatMap { it.getColumnReferences() }
}

/**
 * Represents a SELECT expression (column, literal, or arithmetic expression).
 */
sealed class SelectExpression {
    abstract val alias: String?
    abstract fun getOutputName(): String
    abstract fun getColumnReferences(): List<String>

    /**
     * Simple column reference.
     */
    data class Column(
        val name: String,
        override val alias: String? = null
    ) : SelectExpression() {
        override fun getOutputName(): String = alias ?: name
        override fun getColumnReferences(): List<String> = if (name == "*") emptyList() else listOf(name)
    }

    /**
     * Literal value (number or string).
     */
    data class Literal(
        val value: Any?,
        override val alias: String? = null
    ) : SelectExpression() {
        override fun getOutputName(): String = alias ?: value.toString()
        override fun getColumnReferences(): List<String> = emptyList()
    }

    /**
     * Arithmetic expression.
     */
    data class Arithmetic(
        val left: SelectExpression,
        val operator: ArithmeticOperator,
        val right: SelectExpression,
        override val alias: String? = null,
        val originalExpression: String? = null
    ) : SelectExpression() {
        override fun getOutputName(): String = alias ?: originalExpression ?: "$left $operator $right"
        override fun getColumnReferences(): List<String> = left.getColumnReferences() + right.getColumnReferences()
    }

    /**
     * Function call (e.g., COALESCE, UPPER, etc.)
     */
    data class Function(
        val name: String,
        val arguments: List<SelectExpression>,
        override val alias: String? = null
    ) : SelectExpression() {
        override fun getOutputName(): String = alias ?: "$name(...)"
        override fun getColumnReferences(): List<String> = arguments.flatMap { it.getColumnReferences() }
    }
}

/**
 * Arithmetic operators.
 */
enum class ArithmeticOperator {
    ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO;

    override fun toString(): String = when (this) {
        ADD -> "+"
        SUBTRACT -> "-"
        MULTIPLY -> "*"
        DIVIDE -> "/"
        MODULO -> "%"
    }
}

/**
 * Query without FROM clause (e.g., SELECT 1, SELECT 'hello').
 * Used for ping/keep-alive queries.
 */
data class DualQuery(
    val values: List<Pair<String, Any?>>
) : QueryNode

/**
 * Composite query (UNION, UNION ALL, INTERSECT, EXCEPT).
 */
data class CompositeQuery(
    val queries: List<QueryNode>,
    val operations: List<SetOperation>,
    val orderBy: List<OrderByItem>,
    val limit: Long?
) : QueryNode

/**
 * Joined query with multiple tables.
 */
data class JoinedQuery(
    val leftTable: String,
    val leftAlias: String?,
    val joins: List<JoinInfo>,
    val columns: List<QualifiedColumn>,
    val conditions: List<QualifiedWhereCondition>,
    val limit: Long?,
    val orderBy: List<OrderByItem>,
    val tableAliases: Map<String, String>
) : QueryNode {
    /**
     * Get all table names involved in this query.
     */
    fun getAllTables(): List<String> = listOf(leftTable) + joins.map { it.rightTable }

    /**
     * Get conditions for a specific table.
     */
    fun getConditionsForTable(tableName: String): List<WhereCondition> =
        conditions.filter { it.table == tableName || it.table == null }
            .map { WhereCondition(it.column, it.operator, it.value) }
}

/**
 * Query with a subquery as the FROM source.
 * Example: SELECT * FROM (SELECT * FROM accounts) AS sub
 */
data class SubqueryQuery(
    val subquery: QueryNode,
    val alias: String,
    val selectExpressions: List<SelectExpression>,
    val conditions: List<WhereCondition>,
    val limit: Long?,
    val orderBy: List<OrderByItem>,
    val distinct: Boolean = false
) : QueryNode

/**
 * Query with Common Table Expressions (CTEs).
 * Example: WITH cte AS (SELECT * FROM accounts) SELECT * FROM cte
 */
data class CteQuery(
    val ctes: List<CteDefinition>,
    val mainQuery: QueryNode
) : QueryNode

/**
 * A single CTE definition.
 */
data class CteDefinition(
    val name: String,
    val query: QueryNode
)

/**
 * Set operation type.
 */
enum class SetOperation {
    UNION,
    UNION_ALL,
    INTERSECT,
    EXCEPT
}

/**
 * Join information.
 */
data class JoinInfo(
    val rightTable: String,
    val rightAlias: String?,
    val joinType: JoinType,
    val condition: JoinCondition?
)

/**
 * Join type.
 */
enum class JoinType {
    INNER,
    LEFT,
    RIGHT,
    FULL,
    CROSS,
    NATURAL
}

/**
 * Join condition (column = column).
 */
data class JoinCondition(
    val leftTable: String,
    val leftColumn: String,
    val rightTable: String,
    val rightColumn: String
)

/**
 * Qualified column with optional table and alias.
 */
data class QualifiedColumn(
    val table: String?,
    val column: String,
    val alias: String?
)

/**
 * Qualified WHERE condition with table reference.
 */
data class QualifiedWhereCondition(
    val table: String?,
    val column: String,
    val operator: ComparisonOperator,
    val value: Any?
)

/**
 * WHERE condition representation.
 */
data class WhereCondition(
    val column: String,
    val operator: ComparisonOperator,
    val value: Any?
)

/**
 * Comparison operators.
 */
enum class ComparisonOperator {
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    IN,
    IS_NULL,
    IS_NOT_NULL
}

/**
 * ORDER BY item.
 */
data class OrderByItem(
    val column: String,
    val ascending: Boolean = true
)
