package com.evmjdbc.resultset

import com.evmjdbc.metadata.VirtualSchema
import com.evmjdbc.statement.EvmStatement
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.sql.Array
import java.sql.Date
import java.time.OffsetDateTime
import java.util.*

/**
 * ResultSet implementation for EVM blockchain query results.
 */
class EvmResultSet(
    private val statement: EvmStatement,
    private val tableName: String,
    private val columnNames: List<String>,
    private val rows: List<List<Any?>>
) : ResultSet {

    private var currentRow = -1
    private var closed = false
    private var wasNull = false
    private var fetchDirection = ResultSet.FETCH_FORWARD
    private var fetchSize = 0

    private val columnIndices: Map<String, Int> = columnNames
        .mapIndexed { index, name -> name.lowercase() to index }
        .toMap()

    // ========== Cursor Movement ==========

    override fun next(): Boolean {
        checkOpen()
        if (currentRow < rows.size - 1) {
            currentRow++
            return true
        }
        return false
    }

    override fun previous(): Boolean {
        checkOpen()
        if (currentRow > 0) {
            currentRow--
            return true
        }
        return false
    }

    override fun first(): Boolean {
        checkOpen()
        if (rows.isNotEmpty()) {
            currentRow = 0
            return true
        }
        return false
    }

    override fun last(): Boolean {
        checkOpen()
        if (rows.isNotEmpty()) {
            currentRow = rows.size - 1
            return true
        }
        return false
    }

    override fun beforeFirst() {
        checkOpen()
        currentRow = -1
    }

    override fun afterLast() {
        checkOpen()
        currentRow = rows.size
    }

    override fun absolute(row: Int): Boolean {
        checkOpen()
        return when {
            row > 0 -> {
                currentRow = minOf(row - 1, rows.size)
                currentRow < rows.size
            }
            row < 0 -> {
                currentRow = maxOf(rows.size + row, -1)
                currentRow >= 0
            }
            else -> {
                beforeFirst()
                false
            }
        }
    }

    override fun relative(rows: Int): Boolean {
        checkOpen()
        return absolute(currentRow + 1 + rows)
    }

    override fun isBeforeFirst(): Boolean = currentRow == -1 && rows.isNotEmpty()
    override fun isAfterLast(): Boolean = currentRow >= rows.size && rows.isNotEmpty()
    override fun isFirst(): Boolean = currentRow == 0 && rows.isNotEmpty()
    override fun isLast(): Boolean = currentRow == rows.size - 1 && rows.isNotEmpty()
    override fun getRow(): Int = if (currentRow >= 0 && currentRow < rows.size) currentRow + 1 else 0

    // ========== Get by Column Index ==========

    private fun getValue(columnIndex: Int): Any? {
        checkOpen()
        checkRow()
        if (columnIndex < 1 || columnIndex > columnNames.size) {
            throw SQLException("Invalid column index: $columnIndex")
        }
        val value = rows[currentRow].getOrNull(columnIndex - 1)
        wasNull = value == null
        return value
    }

    override fun getString(columnIndex: Int): String? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is String -> value
            is OffsetDateTime -> value.toString()
            else -> value.toString()
        }
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        val value = getValue(columnIndex) ?: return false
        return when (value) {
            is Boolean -> value
            is Number -> value.toInt() != 0
            is String -> value.equals("true", ignoreCase = true) || value == "1"
            else -> false
        }
    }

    override fun getByte(columnIndex: Int): Byte {
        return (getValue(columnIndex) as? Number)?.toByte() ?: 0
    }

    override fun getShort(columnIndex: Int): Short {
        return (getValue(columnIndex) as? Number)?.toShort() ?: 0
    }

    override fun getInt(columnIndex: Int): Int {
        return (getValue(columnIndex) as? Number)?.toInt() ?: 0
    }

    override fun getLong(columnIndex: Int): Long {
        return (getValue(columnIndex) as? Number)?.toLong() ?: 0
    }

    override fun getFloat(columnIndex: Int): Float {
        return (getValue(columnIndex) as? Number)?.toFloat() ?: 0f
    }

    override fun getDouble(columnIndex: Int): Double {
        return (getValue(columnIndex) as? Number)?.toDouble() ?: 0.0
    }

    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal? {
        return getBigDecimal(columnIndex)?.setScale(scale)
    }

    override fun getBigDecimal(columnIndex: Int): BigDecimal? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is BigDecimal -> value
            is Number -> BigDecimal(value.toString())
            is String -> value.toBigDecimalOrNull()
            else -> null
        }
    }

    override fun getBytes(columnIndex: Int): ByteArray? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is ByteArray -> value
            is String -> {
                // Convert hex string to bytes
                val hex = value.removePrefix("0x")
                ByteArray(hex.length / 2) { i ->
                    Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16).toByte()
                }
            }
            else -> null
        }
    }

    override fun getDate(columnIndex: Int): Date? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is Date -> value
            is OffsetDateTime -> Date(value.toInstant().toEpochMilli())
            is java.util.Date -> Date(value.time)
            is Long -> Date(value * 1000) // Unix timestamp
            else -> null
        }
    }

    override fun getTime(columnIndex: Int): Time? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is Time -> value
            is OffsetDateTime -> Time(value.toInstant().toEpochMilli())
            is java.util.Date -> Time(value.time)
            else -> null
        }
    }

    override fun getTimestamp(columnIndex: Int): Timestamp? {
        val value = getValue(columnIndex) ?: return null
        return when (value) {
            is Timestamp -> value
            is OffsetDateTime -> Timestamp.from(value.toInstant())
            is java.util.Date -> Timestamp(value.time)
            is Long -> Timestamp(value * 1000) // Unix timestamp
            else -> null
        }
    }

    override fun getObject(columnIndex: Int): Any? {
        return getValue(columnIndex)
    }

    override fun getObject(columnIndex: Int, map: MutableMap<String, Class<*>>?): Any? {
        return getValue(columnIndex)
    }

    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>?): T? {
        val value = getValue(columnIndex) ?: return null
        @Suppress("UNCHECKED_CAST")
        return when {
            type == null -> value as? T
            type.isInstance(value) -> value as T
            type == String::class.java -> getString(columnIndex) as? T
            type == Long::class.java || type == java.lang.Long::class.java -> getLong(columnIndex) as? T
            type == Int::class.java || type == java.lang.Integer::class.java -> getInt(columnIndex) as? T
            type == BigDecimal::class.java -> getBigDecimal(columnIndex) as? T
            type == Timestamp::class.java -> getTimestamp(columnIndex) as? T
            type == OffsetDateTime::class.java -> (getValue(columnIndex) as? OffsetDateTime) as? T
            else -> value as? T
        }
    }

    // ========== Get by Column Name ==========

    override fun findColumn(columnLabel: String?): Int {
        checkOpen()
        val index = columnIndices[columnLabel?.lowercase()]
            ?: throw SQLException("Column not found: $columnLabel")
        return index + 1
    }

    override fun getString(columnLabel: String?): String? = getString(findColumn(columnLabel))
    override fun getBoolean(columnLabel: String?): Boolean = getBoolean(findColumn(columnLabel))
    override fun getByte(columnLabel: String?): Byte = getByte(findColumn(columnLabel))
    override fun getShort(columnLabel: String?): Short = getShort(findColumn(columnLabel))
    override fun getInt(columnLabel: String?): Int = getInt(findColumn(columnLabel))
    override fun getLong(columnLabel: String?): Long = getLong(findColumn(columnLabel))
    override fun getFloat(columnLabel: String?): Float = getFloat(findColumn(columnLabel))
    override fun getDouble(columnLabel: String?): Double = getDouble(findColumn(columnLabel))
    @Deprecated("Deprecated in Java")
    override fun getBigDecimal(columnLabel: String?, scale: Int): BigDecimal? = getBigDecimal(findColumn(columnLabel), scale)
    override fun getBigDecimal(columnLabel: String?): BigDecimal? = getBigDecimal(findColumn(columnLabel))
    override fun getBytes(columnLabel: String?): ByteArray? = getBytes(findColumn(columnLabel))
    override fun getDate(columnLabel: String?): Date? = getDate(findColumn(columnLabel))
    override fun getTime(columnLabel: String?): Time? = getTime(findColumn(columnLabel))
    override fun getTimestamp(columnLabel: String?): Timestamp? = getTimestamp(findColumn(columnLabel))
    override fun getObject(columnLabel: String?): Any? = getObject(findColumn(columnLabel))
    override fun getObject(columnLabel: String?, map: MutableMap<String, Class<*>>?): Any? = getObject(findColumn(columnLabel), map)
    override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>?): T? = getObject(findColumn(columnLabel), type)

    // ========== Metadata ==========

    override fun getMetaData(): ResultSetMetaData {
        return EvmResultSetMetaData(tableName, columnNames)
    }

    override fun wasNull(): Boolean = wasNull

    override fun getCursorName(): String? = null

    override fun getType(): Int = ResultSet.TYPE_SCROLL_INSENSITIVE

    override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

    override fun getHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

    override fun getStatement(): Statement = statement

    // ========== Fetch Settings ==========

    override fun setFetchDirection(direction: Int) {
        checkOpen()
        fetchDirection = direction
    }

    override fun getFetchDirection(): Int = fetchDirection

    override fun setFetchSize(rows: Int) {
        checkOpen()
        fetchSize = rows
    }

    override fun getFetchSize(): Int = fetchSize

    // ========== Close ==========

    override fun close() {
        closed = true
    }

    override fun isClosed(): Boolean = closed

    // ========== Unsupported Stream Methods ==========

    override fun getAsciiStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnIndex: Int): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getAsciiStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    @Deprecated("Deprecated in Java")
    override fun getUnicodeStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnLabel: String?): InputStream? = throw SQLFeatureNotSupportedException()
    override fun getWarnings(): SQLWarning? = null
    override fun clearWarnings() {}
    override fun getCharacterStream(columnIndex: Int): Reader? = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnLabel: String?): Reader? = throw SQLFeatureNotSupportedException()

    override fun getRef(columnIndex: Int): Ref? = throw SQLFeatureNotSupportedException()
    override fun getRef(columnLabel: String?): Ref? = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnIndex: Int): Blob? = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnLabel: String?): Blob? = throw SQLFeatureNotSupportedException()
    override fun getClob(columnIndex: Int): Clob? = throw SQLFeatureNotSupportedException()
    override fun getClob(columnLabel: String?): Clob? = throw SQLFeatureNotSupportedException()
    override fun getArray(columnIndex: Int): Array? = throw SQLFeatureNotSupportedException()
    override fun getArray(columnLabel: String?): Array? = throw SQLFeatureNotSupportedException()

    override fun getDate(columnIndex: Int, cal: Calendar?): Date? = getDate(columnIndex)
    override fun getDate(columnLabel: String?, cal: Calendar?): Date? = getDate(columnLabel)
    override fun getTime(columnIndex: Int, cal: Calendar?): Time? = getTime(columnIndex)
    override fun getTime(columnLabel: String?, cal: Calendar?): Time? = getTime(columnLabel)
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp? = getTimestamp(columnIndex)
    override fun getTimestamp(columnLabel: String?, cal: Calendar?): Timestamp? = getTimestamp(columnLabel)

    override fun getURL(columnIndex: Int): URL? = throw SQLFeatureNotSupportedException()
    override fun getURL(columnLabel: String?): URL? = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnIndex: Int): RowId? = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnLabel: String?): RowId? = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnIndex: Int): NClob? = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnLabel: String?): NClob? = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnIndex: Int): SQLXML? = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnLabel: String?): SQLXML? = throw SQLFeatureNotSupportedException()
    override fun getNString(columnIndex: Int): String? = getString(columnIndex)
    override fun getNString(columnLabel: String?): String? = getString(columnLabel)
    override fun getNCharacterStream(columnIndex: Int): Reader? = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnLabel: String?): Reader? = throw SQLFeatureNotSupportedException()

    // ========== Updates (Not Supported) ==========

    override fun rowUpdated(): Boolean = false
    override fun rowInserted(): Boolean = false
    override fun rowDeleted(): Boolean = false

    override fun updateNull(columnIndex: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBoolean(columnIndex: Int, x: Boolean) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateByte(columnIndex: Int, x: Byte) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateShort(columnIndex: Int, x: Short) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateInt(columnIndex: Int, x: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateLong(columnIndex: Int, x: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateFloat(columnIndex: Int, x: Float) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateDouble(columnIndex: Int, x: Double) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateString(columnIndex: Int, x: String?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBytes(columnIndex: Int, x: ByteArray?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateDate(columnIndex: Int, x: Date?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateTime(columnIndex: Int, x: Time?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateObject(columnIndex: Int, x: Any?) = throw SQLFeatureNotSupportedException("Read-only")

    override fun updateNull(columnLabel: String?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBoolean(columnLabel: String?, x: Boolean) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateByte(columnLabel: String?, x: Byte) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateShort(columnLabel: String?, x: Short) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateInt(columnLabel: String?, x: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateLong(columnLabel: String?, x: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateFloat(columnLabel: String?, x: Float) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateDouble(columnLabel: String?, x: Double) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBigDecimal(columnLabel: String?, x: BigDecimal?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateString(columnLabel: String?, x: String?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBytes(columnLabel: String?, x: ByteArray?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateDate(columnLabel: String?, x: Date?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateTime(columnLabel: String?, x: Time?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateTimestamp(columnLabel: String?, x: Timestamp?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, x: Reader?, length: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateObject(columnLabel: String?, x: Any?, scaleOrLength: Int) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateObject(columnLabel: String?, x: Any?) = throw SQLFeatureNotSupportedException("Read-only")

    override fun insertRow() = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateRow() = throw SQLFeatureNotSupportedException("Read-only")
    override fun deleteRow() = throw SQLFeatureNotSupportedException("Read-only")
    override fun refreshRow() = throw SQLFeatureNotSupportedException("Read-only")
    override fun cancelRowUpdates() = throw SQLFeatureNotSupportedException("Read-only")
    override fun moveToInsertRow() = throw SQLFeatureNotSupportedException("Read-only")
    override fun moveToCurrentRow() {}

    override fun updateRef(columnIndex: Int, x: Ref?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateRef(columnLabel: String?, x: Ref?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnIndex: Int, x: Blob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnLabel: String?, x: Blob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBlob(columnLabel: String?, inputStream: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnIndex: Int, x: Clob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnLabel: String?, x: Clob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateArray(columnIndex: Int, x: Array?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateArray(columnLabel: String?, x: Array?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateRowId(columnIndex: Int, x: RowId?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateRowId(columnLabel: String?, x: RowId?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNString(columnIndex: Int, nString: String?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNString(columnLabel: String?, nString: String?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnIndex: Int, nClob: NClob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnLabel: String?, nClob: NClob?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnIndex: Int, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNClob(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateSQLXML(columnLabel: String?, xmlObject: SQLXML?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateNCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateAsciiStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateBinaryStream(columnLabel: String?, x: InputStream?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?, length: Long) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) = throw SQLFeatureNotSupportedException("Read-only")
    override fun updateCharacterStream(columnLabel: String?, reader: Reader?) = throw SQLFeatureNotSupportedException("Read-only")

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        if (iface != null && iface.isInstance(this)) {
            @Suppress("UNCHECKED_CAST")
            return this as T
        }
        throw SQLException("Cannot unwrap to ${iface?.name}")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean = iface?.isInstance(this) == true

    // ========== Private Helpers ==========

    private fun checkOpen() {
        if (closed) throw SQLException("ResultSet is closed")
    }

    private fun checkRow() {
        if (currentRow < 0 || currentRow >= rows.size) {
            throw SQLException("No current row")
        }
    }
}
