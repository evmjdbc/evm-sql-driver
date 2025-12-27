package com.evmjdbc.contract

import com.evmjdbc.connection.ConnectionProperties
import com.evmjdbc.connection.EvmConnection
import com.evmjdbc.metadata.VirtualSchema
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.Types

/**
 * JDBC compliance tests for DatabaseMetaData.
 * Verifies that getTables, getColumns, getSchemas work correctly for DataGrip.
 */
@DisplayName("JDBC Compliance - Metadata")
class MetadataComplianceTest {

    private lateinit var connection: EvmConnection
    private lateinit var metadata: DatabaseMetaData

    @BeforeEach
    fun setup() {
        // Create a connection with mock RPC
        val props = ConnectionProperties(
            rpcUrl = "http://localhost:8545",
            network = "test"
        )
        connection = spyk(EvmConnection(props))
        metadata = connection.metaData
    }

    @AfterEach
    fun teardown() {
        connection.close()
    }

    // ========== Schema Tests ==========

    @Test
    @DisplayName("getSchemas returns evm schema")
    fun getSchemasReturnsEvmSchema() {
        val rs = metadata.schemas

        assertTrue(rs.next(), "Should have at least one schema")
        assertEquals(VirtualSchema.SCHEMA_NAME, rs.getString("TABLE_SCHEM"))

        assertFalse(rs.next(), "Should only have one schema")
        rs.close()
    }

    @Test
    @DisplayName("getSchemaTerm returns 'schema'")
    fun getSchemaTerm() {
        assertEquals("schema", metadata.schemaTerm)
    }

    // ========== Table Types Tests ==========

    @Test
    @DisplayName("getTableTypes returns TABLE, VIEW, SYSTEM TABLE")
    fun getTableTypesReturnsAllTypes() {
        val rs = metadata.tableTypes
        val types = mutableListOf<String>()

        while (rs.next()) {
            types.add(rs.getString("TABLE_TYPE"))
        }
        rs.close()

        assertTrue(types.contains("TABLE"), "Should include TABLE")
        assertTrue(types.contains("VIEW"), "Should include VIEW")
        assertTrue(types.contains("SYSTEM TABLE"), "Should include SYSTEM TABLE")
    }

    // ========== Tables Tests ==========

    @Test
    @DisplayName("getTables returns all virtual tables")
    fun getTablesReturnsAllTables() {
        val rs = metadata.getTables(null, null, "%", null)
        val tables = mutableListOf<String>()

        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"))
        }
        rs.close()

        // Verify expected tables are present
        assertTrue(tables.contains("blocks"), "Should include blocks table")
        assertTrue(tables.contains("transactions"), "Should include transactions table")
        assertTrue(tables.contains("logs"), "Should include logs table")
        assertTrue(tables.contains("accounts"), "Should include accounts table")
        assertTrue(tables.contains("chain_info"), "Should include chain_info table")
    }

    @Test
    @DisplayName("getTables respects table name pattern")
    fun getTablesRespectsPattern() {
        val rs = metadata.getTables(null, null, "block%", null)
        val tables = mutableListOf<String>()

        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"))
        }
        rs.close()

        assertTrue(tables.contains("blocks"))
        assertFalse(tables.contains("transactions"))
    }

    @Test
    @DisplayName("getTables respects type filter")
    fun getTablesRespectsTypeFilter() {
        val rs = metadata.getTables(null, null, "%", arrayOf("VIEW"))
        val tables = mutableListOf<String>()

        while (rs.next()) {
            tables.add(rs.getString("TABLE_NAME"))
        }
        rs.close()

        assertTrue(tables.contains("erc20_transfers"), "Should include erc20_transfers view")
        assertFalse(tables.contains("blocks"), "Should not include blocks table")
    }

    @Test
    @DisplayName("getTables returns correct metadata columns")
    fun getTablesReturnsCorrectColumns() {
        val rs = metadata.getTables(null, null, "blocks", null)

        assertTrue(rs.next())
        assertEquals(VirtualSchema.SCHEMA_NAME, rs.getString("TABLE_SCHEM"))
        assertEquals("blocks", rs.getString("TABLE_NAME"))
        assertEquals("TABLE", rs.getString("TABLE_TYPE"))
        assertNotNull(rs.getString("REMARKS"))

        rs.close()
    }

    // ========== Columns Tests ==========

    @Test
    @DisplayName("getColumns returns columns for blocks table")
    fun getColumnsForBlocksTable() {
        val rs = metadata.getColumns(null, null, "blocks", "%")
        val columns = mutableListOf<String>()

        while (rs.next()) {
            columns.add(rs.getString("COLUMN_NAME"))
        }
        rs.close()

        assertTrue(columns.contains("number"), "Should include number column")
        assertTrue(columns.contains("hash"), "Should include hash column")
        assertTrue(columns.contains("miner"), "Should include miner column")
        assertTrue(columns.contains("timestamp"), "Should include timestamp column")
    }

    @Test
    @DisplayName("getColumns returns correct column metadata")
    fun getColumnsReturnsCorrectMetadata() {
        val rs = metadata.getColumns(null, null, "blocks", "number")

        assertTrue(rs.next())
        assertEquals("blocks", rs.getString("TABLE_NAME"))
        assertEquals("number", rs.getString("COLUMN_NAME"))
        assertEquals(Types.BIGINT, rs.getInt("DATA_TYPE"))
        assertEquals("BIGINT", rs.getString("TYPE_NAME"))
        assertEquals(DatabaseMetaData.columnNoNulls, rs.getInt("NULLABLE"))
        assertEquals("NO", rs.getString("IS_NULLABLE"))

        rs.close()
    }

    @Test
    @DisplayName("getColumns respects column name pattern")
    fun getColumnsRespectsPattern() {
        val rs = metadata.getColumns(null, null, "blocks", "gas%")
        val columns = mutableListOf<String>()

        while (rs.next()) {
            columns.add(rs.getString("COLUMN_NAME"))
        }
        rs.close()

        assertTrue(columns.contains("gas_limit"))
        assertTrue(columns.contains("gas_used"))
        assertFalse(columns.contains("number"))
    }

    @Test
    @DisplayName("getColumns returns ordinal positions")
    fun getColumnsReturnsOrdinalPositions() {
        val rs = metadata.getColumns(null, null, "blocks", "%")
        var prevOrdinal = 0

        while (rs.next()) {
            val ordinal = rs.getInt("ORDINAL_POSITION")
            assertTrue(ordinal > prevOrdinal, "Ordinal positions should increase")
            prevOrdinal = ordinal
        }
        rs.close()
    }

    // ========== Primary Keys Tests ==========

    @Test
    @DisplayName("getPrimaryKeys returns primary key for blocks table")
    fun getPrimaryKeysForBlocks() {
        val rs = metadata.getPrimaryKeys(null, null, "blocks")

        assertTrue(rs.next())
        assertEquals("blocks", rs.getString("TABLE_NAME"))
        assertEquals("number", rs.getString("COLUMN_NAME"))
        assertEquals(1, rs.getShort("KEY_SEQ"))

        assertFalse(rs.next(), "blocks should have single-column PK")
        rs.close()
    }

    @Test
    @DisplayName("getPrimaryKeys returns composite key for logs table")
    fun getPrimaryKeysForLogs() {
        val rs = metadata.getPrimaryKeys(null, null, "logs")
        val keyColumns = mutableListOf<String>()

        while (rs.next()) {
            assertEquals("logs", rs.getString("TABLE_NAME"))
            keyColumns.add(rs.getString("COLUMN_NAME"))
        }
        rs.close()

        assertTrue(keyColumns.contains("block_number"))
        assertTrue(keyColumns.contains("log_index"))
        assertEquals(2, keyColumns.size)
    }

    // ========== Type Info Tests ==========

    @Test
    @DisplayName("getTypeInfo returns supported types")
    fun getTypeInfoReturnsSupportedTypes() {
        val rs = metadata.typeInfo
        val types = mutableListOf<String>()

        while (rs.next()) {
            types.add(rs.getString("TYPE_NAME"))
        }
        rs.close()

        assertTrue(types.contains("VARCHAR"))
        assertTrue(types.contains("BIGINT"))
        assertTrue(types.contains("DECIMAL"))
        assertTrue(types.contains("BOOLEAN"))
        assertTrue(types.contains("TIMESTAMP WITH TIME ZONE"))
    }

    // ========== Feature Support Tests ==========

    @Test
    @DisplayName("isReadOnly returns true")
    fun isReadOnlyReturnsTrue() {
        assertTrue(metadata.isReadOnly)
    }

    @Test
    @DisplayName("supportsTransactions returns false")
    fun supportsTransactionsReturnsFalse() {
        assertFalse(metadata.supportsTransactions())
    }

    @Test
    @DisplayName("allTablesAreSelectable returns true")
    fun allTablesAreSelectableReturnsTrue() {
        assertTrue(metadata.allTablesAreSelectable())
    }

    @Test
    @DisplayName("supportsGroupBy returns true")
    fun supportsGroupByReturnsTrue() {
        assertTrue(metadata.supportsGroupBy())
    }

    @Test
    @DisplayName("getJDBCMajorVersion returns 4")
    fun jdbcMajorVersion() {
        assertEquals(4, metadata.jdbcMajorVersion)
    }

    @Test
    @DisplayName("getJDBCMinorVersion returns 2")
    fun jdbcMinorVersion() {
        assertEquals(2, metadata.jdbcMinorVersion)
    }

    // ========== Connection Tests ==========

    @Test
    @DisplayName("getConnection returns the connection")
    fun getConnectionReturnsConnection() {
        assertSame(connection, metadata.connection)
    }

    @Test
    @DisplayName("getDatabaseProductName returns EVM Blockchain")
    fun getDatabaseProductName() {
        assertEquals("EVM Blockchain", metadata.databaseProductName)
    }

    @Test
    @DisplayName("getDriverName returns EVM SQL Driver")
    fun getDriverName() {
        assertEquals("EVM SQL Driver", metadata.driverName)
    }
}
