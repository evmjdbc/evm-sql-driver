package com.evmjdbc.statement

import com.evmjdbc.connection.EvmConnection
import com.evmjdbc.exceptions.ErrorCode
import com.evmjdbc.exceptions.EvmSqlException
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.util.*

/**
 * JDBC PreparedStatement implementation for EVM SQL queries.
 * Supports parameterized queries with ? placeholders.
 */
class EvmPreparedStatement(
    connection: EvmConnection,
    private val sql: String
) : EvmStatement(connection), PreparedStatement {

    private val parameters = mutableMapOf<Int, Any?>()
    private var parameterMetaData: ParameterMetaData? = null

    /**
     * Execute the prepared query.
     */
    override fun executeQuery(): ResultSet {
        checkOpen()
        val resolvedSql = resolveSql()
        return executeQuery(resolvedSql)
    }

    /**
     * Execute a prepared update - always throws for read-only driver.
     */
    override fun executeUpdate(): Int {
        checkOpen()
        throw EvmSqlException(
            ErrorCode.READ_ONLY_VIOLATION,
            "INSERT, UPDATE, DELETE not supported. Use eth_sendRawTransaction via RPC directly."
        )
    }

    override fun execute(): Boolean {
        checkOpen()
        val resolvedSql = resolveSql()
        return execute(resolvedSql)
    }

    /**
     * Resolve SQL with parameter values.
     */
    private fun resolveSql(): String {
        var result = sql
        var paramIndex = 1
        val regex = Regex("\\?")

        return regex.replace(result) {
            val value = parameters[paramIndex++]
            formatValue(value)
        }
    }

    /**
     * Format a parameter value for SQL.
     */
    private fun formatValue(value: Any?): String {
        return when (value) {
            null -> "NULL"
            is String -> "'${value.replace("'", "''")}'"
            is Number -> value.toString()
            is Boolean -> if (value) "TRUE" else "FALSE"
            is Date -> "'${value}'"
            is Time -> "'${value}'"
            is Timestamp -> "'${value}'"
            is ByteArray -> "0x${value.joinToString("") { "%02x".format(it) }}"
            else -> "'${value.toString().replace("'", "''")}'"
        }
    }

    override fun clearParameters() {
        parameters.clear()
    }

    // ========== Set Methods ==========

    override fun setNull(parameterIndex: Int, sqlType: Int) {
        parameters[parameterIndex] = null
    }

    override fun setNull(parameterIndex: Int, sqlType: Int, typeName: String?) {
        parameters[parameterIndex] = null
    }

    override fun setBoolean(parameterIndex: Int, x: Boolean) {
        parameters[parameterIndex] = x
    }

    override fun setByte(parameterIndex: Int, x: Byte) {
        parameters[parameterIndex] = x
    }

    override fun setShort(parameterIndex: Int, x: Short) {
        parameters[parameterIndex] = x
    }

    override fun setInt(parameterIndex: Int, x: Int) {
        parameters[parameterIndex] = x
    }

    override fun setLong(parameterIndex: Int, x: Long) {
        parameters[parameterIndex] = x
    }

    override fun setFloat(parameterIndex: Int, x: Float) {
        parameters[parameterIndex] = x
    }

    override fun setDouble(parameterIndex: Int, x: Double) {
        parameters[parameterIndex] = x
    }

    override fun setBigDecimal(parameterIndex: Int, x: BigDecimal?) {
        parameters[parameterIndex] = x
    }

    override fun setString(parameterIndex: Int, x: String?) {
        parameters[parameterIndex] = x
    }

    override fun setBytes(parameterIndex: Int, x: ByteArray?) {
        parameters[parameterIndex] = x
    }

    override fun setDate(parameterIndex: Int, x: Date?) {
        parameters[parameterIndex] = x
    }

    override fun setDate(parameterIndex: Int, x: Date?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    override fun setTime(parameterIndex: Int, x: Time?) {
        parameters[parameterIndex] = x
    }

    override fun setTime(parameterIndex: Int, x: Time?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?) {
        parameters[parameterIndex] = x
    }

    override fun setTimestamp(parameterIndex: Int, x: Timestamp?, cal: Calendar?) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
        parameters[parameterIndex] = x
    }

    override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
        parameters[parameterIndex] = x
    }

    // ========== Stream Methods (Limited Support) ==========

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setAsciiStream(parameterIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    @Deprecated("Deprecated in Java")
    override fun setUnicodeStream(parameterIndex: Int, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Int) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setBinaryStream(parameterIndex: Int, x: InputStream?) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Int) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setCharacterStream(parameterIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    override fun setNCharacterStream(parameterIndex: Int, value: Reader?) {
        throw SQLFeatureNotSupportedException("Stream parameters not supported")
    }

    // ========== Unsupported Types ==========

    override fun setRef(parameterIndex: Int, x: Ref?) {
        throw SQLFeatureNotSupportedException("Ref not supported")
    }

    override fun setBlob(parameterIndex: Int, x: Blob?) {
        throw SQLFeatureNotSupportedException("Blob not supported")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?, length: Long) {
        throw SQLFeatureNotSupportedException("Blob not supported")
    }

    override fun setBlob(parameterIndex: Int, inputStream: InputStream?) {
        throw SQLFeatureNotSupportedException("Blob not supported")
    }

    override fun setClob(parameterIndex: Int, x: Clob?) {
        throw SQLFeatureNotSupportedException("Clob not supported")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("Clob not supported")
    }

    override fun setClob(parameterIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("Clob not supported")
    }

    override fun setNClob(parameterIndex: Int, value: NClob?) {
        throw SQLFeatureNotSupportedException("NClob not supported")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?, length: Long) {
        throw SQLFeatureNotSupportedException("NClob not supported")
    }

    override fun setNClob(parameterIndex: Int, reader: Reader?) {
        throw SQLFeatureNotSupportedException("NClob not supported")
    }

    override fun setArray(parameterIndex: Int, x: Array?) {
        throw SQLFeatureNotSupportedException("Array not supported")
    }

    override fun setURL(parameterIndex: Int, x: URL?) {
        throw SQLFeatureNotSupportedException("URL not supported")
    }

    override fun setRowId(parameterIndex: Int, x: RowId?) {
        throw SQLFeatureNotSupportedException("RowId not supported")
    }

    override fun setNString(parameterIndex: Int, value: String?) {
        parameters[parameterIndex] = value
    }

    override fun setSQLXML(parameterIndex: Int, xmlObject: SQLXML?) {
        throw SQLFeatureNotSupportedException("SQLXML not supported")
    }

    // ========== Metadata ==========

    override fun getMetaData(): ResultSetMetaData? {
        // Cannot determine metadata without executing query
        return null
    }

    override fun getParameterMetaData(): ParameterMetaData {
        return parameterMetaData ?: SimpleParameterMetaData(countParameters())
    }

    private fun countParameters(): Int {
        return sql.count { it == '?' }
    }

    // ========== Batch (Not Supported for Read-Only) ==========

    override fun addBatch() {
        throw SQLFeatureNotSupportedException("Batch operations not supported for read-only driver")
    }
}

/**
 * Simple ParameterMetaData implementation.
 */
private class SimpleParameterMetaData(private val paramCount: Int) : ParameterMetaData {

    override fun getParameterCount(): Int = paramCount

    override fun isNullable(param: Int): Int = ParameterMetaData.parameterNullable

    override fun isSigned(param: Int): Boolean = true

    override fun getPrecision(param: Int): Int = 0

    override fun getScale(param: Int): Int = 0

    override fun getParameterType(param: Int): Int = Types.VARCHAR

    override fun getParameterTypeName(param: Int): String = "VARCHAR"

    override fun getParameterClassName(param: Int): String = String::class.java.name

    override fun getParameterMode(param: Int): Int = ParameterMetaData.parameterModeIn

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
}
