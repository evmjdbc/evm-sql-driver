package com.evmjdbc.resultset

import com.evmjdbc.metadata.VirtualSchema
import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * ResultSetMetaData implementation for EVM query results.
 * Handles both regular table queries and composite results (UNION, JOIN).
 */
class EvmResultSetMetaData(
    private val tableName: String,
    private val columnNames: List<String>
) : ResultSetMetaData {

    private val tableDef = VirtualSchema.getTable(tableName)

    // For composite results, try to find column definitions from any table
    private val columnDefs = columnNames.map { name ->
        findColumnDef(name)
    }

    /**
     * Find column definition by name, checking multiple sources.
     */
    private fun findColumnDef(name: String): ColumnInfo {
        // Strip table prefix if present (e.g., "blocks.number" -> "number")
        val cleanName = name.substringAfterLast(".")

        // First try the specific table
        tableDef?.columns?.find { it.name.equals(cleanName, ignoreCase = true) }?.let {
            return ColumnInfo(it.name, it.sqlType, it.typeName, it.precision, it.scale, it.nullable)
        }

        // For composite results, check if name contains table prefix
        if (name.contains(".")) {
            val tablePrefix = name.substringBeforeLast(".")
            VirtualSchema.getTable(tablePrefix)?.columns?.find {
                it.name.equals(cleanName, ignoreCase = true)
            }?.let {
                return ColumnInfo(name, it.sqlType, it.typeName, it.precision, it.scale, it.nullable)
            }
        }

        // Search all tables for the column
        for (table in VirtualSchema.ALL_TABLES) {
            table.columns.find { col -> col.name.equals(cleanName, ignoreCase = true) }?.let { col ->
                return ColumnInfo(name, col.sqlType, col.typeName, col.precision, col.scale, col.nullable)
            }
        }

        // Fallback: infer type from column name patterns
        return inferColumnInfo(name)
    }

    /**
     * Infer column info based on common naming patterns.
     */
    private fun inferColumnInfo(name: String): ColumnInfo {
        val lowerName = name.lowercase().substringAfterLast(".")

        return when {
            lowerName.endsWith("_number") || lowerName == "number" || lowerName == "nonce" ->
                ColumnInfo(name, java.sql.Types.BIGINT, "BIGINT", 19, 0, false)

            lowerName.endsWith("_hash") || lowerName == "hash" ->
                ColumnInfo(name, java.sql.Types.VARCHAR, "VARCHAR", 66, 0, false)

            lowerName.endsWith("_address") || lowerName == "address" || lowerName == "miner" ->
                ColumnInfo(name, java.sql.Types.VARCHAR, "VARCHAR", 42, 0, false)

            lowerName == "value" || lowerName == "balance" || lowerName.endsWith("_fee") ||
            lowerName == "gas_price" || lowerName == "amount" ->
                ColumnInfo(name, java.sql.Types.DECIMAL, "DECIMAL", 78, 0, false)

            lowerName == "timestamp" ->
                ColumnInfo(name, java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE", 29, 0, false)

            lowerName.startsWith("is_") || lowerName == "removed" || lowerName == "supported" ->
                ColumnInfo(name, java.sql.Types.BOOLEAN, "BOOLEAN", 1, 0, false)

            lowerName.endsWith("_index") || lowerName.endsWith("_count") || lowerName == "type" || lowerName == "status" ->
                ColumnInfo(name, java.sql.Types.INTEGER, "INTEGER", 10, 0, false)

            lowerName.endsWith("_id") ->
                ColumnInfo(name, java.sql.Types.DECIMAL, "DECIMAL", 78, 0, false)

            else ->
                ColumnInfo(name, java.sql.Types.VARCHAR, "VARCHAR", 2000, 0, true)
        }
    }

    override fun getColumnCount(): Int = columnNames.size

    override fun isAutoIncrement(column: Int): Boolean = false

    override fun isCaseSensitive(column: Int): Boolean {
        checkColumn(column)
        return false
    }

    override fun isSearchable(column: Int): Boolean {
        checkColumn(column)
        return true
    }

    override fun isCurrency(column: Int): Boolean = false

    override fun isNullable(column: Int): Int {
        checkColumn(column)
        return if (columnDefs[column - 1].nullable) {
            ResultSetMetaData.columnNullable
        } else {
            ResultSetMetaData.columnNoNulls
        }
    }

    override fun isSigned(column: Int): Boolean {
        checkColumn(column)
        return when (columnDefs[column - 1].sqlType) {
            java.sql.Types.BIGINT, java.sql.Types.INTEGER,
            java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> true
            else -> false
        }
    }

    override fun getColumnDisplaySize(column: Int): Int {
        checkColumn(column)
        return columnDefs[column - 1].precision
    }

    override fun getColumnLabel(column: Int): String {
        checkColumn(column)
        return columnNames[column - 1]
    }

    override fun getColumnName(column: Int): String {
        checkColumn(column)
        return columnNames[column - 1]
    }

    override fun getSchemaName(column: Int): String {
        checkColumn(column)
        return VirtualSchema.SCHEMA_NAME
    }

    override fun getPrecision(column: Int): Int {
        checkColumn(column)
        return columnDefs[column - 1].precision
    }

    override fun getScale(column: Int): Int {
        checkColumn(column)
        return columnDefs[column - 1].scale
    }

    override fun getTableName(column: Int): String {
        checkColumn(column)
        return tableName
    }

    override fun getCatalogName(column: Int): String {
        checkColumn(column)
        return VirtualSchema.CATALOG_NAME
    }

    override fun getColumnType(column: Int): Int {
        checkColumn(column)
        return columnDefs[column - 1].sqlType
    }

    override fun getColumnTypeName(column: Int): String {
        checkColumn(column)
        return columnDefs[column - 1].typeName
    }

    override fun isReadOnly(column: Int): Boolean = true

    override fun isWritable(column: Int): Boolean = false

    override fun isDefinitelyWritable(column: Int): Boolean = false

    override fun getColumnClassName(column: Int): String {
        checkColumn(column)
        return when (columnDefs[column - 1].sqlType) {
            java.sql.Types.BIGINT -> java.lang.Long::class.java.name
            java.sql.Types.INTEGER -> java.lang.Integer::class.java.name
            java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> java.math.BigDecimal::class.java.name
            java.sql.Types.VARCHAR, java.sql.Types.CHAR -> java.lang.String::class.java.name
            java.sql.Types.BOOLEAN -> java.lang.Boolean::class.java.name
            java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> java.time.OffsetDateTime::class.java.name
            java.sql.Types.TIMESTAMP -> java.sql.Timestamp::class.java.name
            else -> java.lang.Object::class.java.name
        }
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        if (iface != null && iface.isInstance(this)) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLException("Cannot unwrap to ${iface?.name}")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean = iface?.isInstance(this) == true

    private fun checkColumn(column: Int) {
        if (column < 1 || column > columnNames.size) {
            throw SQLException("Invalid column index: $column")
        }
    }

    /**
     * Internal column info holder.
     */
    private data class ColumnInfo(
        val name: String,
        val sqlType: Int,
        val typeName: String,
        val precision: Int,
        val scale: Int,
        val nullable: Boolean
    )
}
