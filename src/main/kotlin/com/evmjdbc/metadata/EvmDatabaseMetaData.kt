package com.evmjdbc.metadata

import com.evmjdbc.Driver
import com.evmjdbc.connection.EvmConnection
import com.evmjdbc.resultset.SimpleResultSet
import java.sql.*

/**
 * DatabaseMetaData implementation for EVM blockchain virtual schema.
 * Provides table, column, and driver information to JDBC clients (like DataGrip).
 */
class EvmDatabaseMetaData(
    private val connection: EvmConnection
) : DatabaseMetaData {

    // ========== Driver Information ==========

    override fun getDriverName(): String = Driver.DRIVER_NAME

    override fun getDriverVersion(): String = Driver.DRIVER_VERSION

    override fun getDriverMajorVersion(): Int = Driver.MAJOR_VERSION

    override fun getDriverMinorVersion(): Int = Driver.MINOR_VERSION

    // ========== Database Information ==========

    override fun getDatabaseProductName(): String = "EVM Blockchain"

    override fun getDatabaseProductVersion(): String {
        val chainId = connection.getChainId()
        return chainId?.let { getNetworkName(it) } ?: "Unknown Chain"
    }

    private fun getNetworkName(chainId: Long): String = when (chainId) {
        1L -> "Ethereum Mainnet"
        5L -> "Goerli Testnet"
        11155111L -> "Sepolia Testnet"
        137L -> "Polygon Mainnet"
        80001L -> "Polygon Mumbai"
        42161L -> "Arbitrum One"
        421613L -> "Arbitrum Goerli"
        10L -> "Optimism Mainnet"
        420L -> "Optimism Goerli"
        56L -> "BNB Smart Chain"
        97L -> "BNB Testnet"
        43114L -> "Avalanche C-Chain"
        43113L -> "Avalanche Fuji"
        8453L -> "Base Mainnet"
        84531L -> "Base Goerli"
        else -> "Chain ID: $chainId"
    }

    override fun getDatabaseMajorVersion(): Int = 1

    override fun getDatabaseMinorVersion(): Int = 0

    override fun getURL(): String? = connection.connectionProperties.rpcUrl

    override fun getUserName(): String? = null // No authentication user

    override fun getConnection(): Connection = connection

    // ========== JDBC Compliance ==========

    override fun getJDBCMajorVersion(): Int = 4

    override fun getJDBCMinorVersion(): Int = 2

    // ========== Catalog and Schema Support ==========

    override fun supportsCatalogsInDataManipulation(): Boolean = false

    override fun supportsCatalogsInProcedureCalls(): Boolean = false

    override fun supportsCatalogsInTableDefinitions(): Boolean = false

    override fun supportsCatalogsInIndexDefinitions(): Boolean = false

    override fun supportsCatalogsInPrivilegeDefinitions(): Boolean = false

    override fun supportsSchemasInDataManipulation(): Boolean = true

    override fun supportsSchemasInProcedureCalls(): Boolean = false

    override fun supportsSchemasInTableDefinitions(): Boolean = true

    override fun supportsSchemasInIndexDefinitions(): Boolean = false

    override fun supportsSchemasInPrivilegeDefinitions(): Boolean = false

    override fun getCatalogSeparator(): String = "."

    override fun getCatalogTerm(): String = "catalog"

    override fun getSchemaTerm(): String = "schema"

    override fun isCatalogAtStart(): Boolean = true

    override fun getCatalogs(): ResultSet {
        // Return empty catalog list (we use schema instead)
        val columns = listOf(
            ResultSetColumn("TABLE_CAT", Types.VARCHAR, 128)
        )
        return SimpleResultSet(columns, emptyList())
    }

    /**
     * Get list of schemas (only "evm").
     */
    override fun getSchemas(): ResultSet {
        val columns = listOf(
            ResultSetColumn("TABLE_SCHEM", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_CATALOG", Types.VARCHAR, 128)
        )
        val rows = listOf(
            listOf(VirtualSchema.SCHEMA_NAME, VirtualSchema.CATALOG_NAME.ifEmpty { null })
        )
        return SimpleResultSet(columns, rows)
    }

    override fun getSchemas(catalog: String?, schemaPattern: String?): ResultSet {
        if (schemaPattern != null && !VirtualSchema.SCHEMA_NAME.contains(schemaPattern.replace("%", ""), ignoreCase = true)) {
            return SimpleResultSet(
                listOf(
                    ResultSetColumn("TABLE_SCHEM", Types.VARCHAR, 128),
                    ResultSetColumn("TABLE_CATALOG", Types.VARCHAR, 128)
                ),
                emptyList()
            )
        }
        return getSchemas()
    }

    /**
     * Get list of table types.
     */
    override fun getTableTypes(): ResultSet {
        val columns = listOf(
            ResultSetColumn("TABLE_TYPE", Types.VARCHAR, 32)
        )
        val rows = listOf(
            listOf(VirtualSchema.TableTypes.TABLE),
            listOf(VirtualSchema.TableTypes.VIEW),
            listOf(VirtualSchema.TableTypes.SYSTEM_TABLE)
        )
        return SimpleResultSet(columns, rows)
    }

    /**
     * Get list of tables matching the pattern.
     */
    override fun getTables(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        types: Array<out String>?
    ): ResultSet {
        val columns = listOf(
            ResultSetColumn("TABLE_CAT", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_SCHEM", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_TYPE", Types.VARCHAR, 32),
            ResultSetColumn("REMARKS", Types.VARCHAR, 512),
            ResultSetColumn("TYPE_CAT", Types.VARCHAR, 128),
            ResultSetColumn("TYPE_SCHEM", Types.VARCHAR, 128),
            ResultSetColumn("TYPE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("SELF_REFERENCING_COL_NAME", Types.VARCHAR, 128),
            ResultSetColumn("REF_GENERATION", Types.VARCHAR, 32)
        )

        val typeFilter = types?.toSet()
        val rows = VirtualSchema.ALL_TABLES
            .filter { table ->
                // Filter by schema pattern
                (schemaPattern == null || schemaPattern == "%" ||
                        VirtualSchema.SCHEMA_NAME.equals(schemaPattern, ignoreCase = true)) &&
                        // Filter by table name pattern
                        (tableNamePattern == null || tableNamePattern == "%" ||
                                matchesPattern(table.name, tableNamePattern)) &&
                        // Filter by table type
                        (typeFilter == null || table.type in typeFilter)
            }
            .map { table ->
                listOf(
                    VirtualSchema.CATALOG_NAME.ifEmpty { null },  // TABLE_CAT
                    VirtualSchema.SCHEMA_NAME,                     // TABLE_SCHEM
                    table.name,                                    // TABLE_NAME
                    table.type,                                    // TABLE_TYPE
                    table.description,                             // REMARKS
                    null,                                          // TYPE_CAT
                    null,                                          // TYPE_SCHEM
                    null,                                          // TYPE_NAME
                    null,                                          // SELF_REFERENCING_COL_NAME
                    null                                           // REF_GENERATION
                )
            }

        return SimpleResultSet(columns, rows)
    }

    /**
     * Get columns for the specified table.
     */
    override fun getColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        val columns = listOf(
            ResultSetColumn("TABLE_CAT", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_SCHEM", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("COLUMN_NAME", Types.VARCHAR, 128),
            ResultSetColumn("DATA_TYPE", Types.INTEGER, 10),
            ResultSetColumn("TYPE_NAME", Types.VARCHAR, 64),
            ResultSetColumn("COLUMN_SIZE", Types.INTEGER, 10),
            ResultSetColumn("BUFFER_LENGTH", Types.INTEGER, 10),
            ResultSetColumn("DECIMAL_DIGITS", Types.INTEGER, 10),
            ResultSetColumn("NUM_PREC_RADIX", Types.INTEGER, 10),
            ResultSetColumn("NULLABLE", Types.INTEGER, 10),
            ResultSetColumn("REMARKS", Types.VARCHAR, 512),
            ResultSetColumn("COLUMN_DEF", Types.VARCHAR, 256),
            ResultSetColumn("SQL_DATA_TYPE", Types.INTEGER, 10),
            ResultSetColumn("SQL_DATETIME_SUB", Types.INTEGER, 10),
            ResultSetColumn("CHAR_OCTET_LENGTH", Types.INTEGER, 10),
            ResultSetColumn("ORDINAL_POSITION", Types.INTEGER, 10),
            ResultSetColumn("IS_NULLABLE", Types.VARCHAR, 3),
            ResultSetColumn("SCOPE_CATALOG", Types.VARCHAR, 128),
            ResultSetColumn("SCOPE_SCHEMA", Types.VARCHAR, 128),
            ResultSetColumn("SCOPE_TABLE", Types.VARCHAR, 128),
            ResultSetColumn("SOURCE_DATA_TYPE", Types.SMALLINT, 5),
            ResultSetColumn("IS_AUTOINCREMENT", Types.VARCHAR, 3),
            ResultSetColumn("IS_GENERATEDCOLUMN", Types.VARCHAR, 3)
        )

        val rows = mutableListOf<List<Any?>>()

        VirtualSchema.ALL_TABLES
            .filter { table ->
                (schemaPattern == null || schemaPattern == "%" ||
                        VirtualSchema.SCHEMA_NAME.equals(schemaPattern, ignoreCase = true)) &&
                        (tableNamePattern == null || tableNamePattern == "%" ||
                                matchesPattern(table.name, tableNamePattern))
            }
            .forEach { table ->
                table.columns.forEachIndexed { index, col ->
                    if (columnNamePattern == null || columnNamePattern == "%" ||
                        matchesPattern(col.name, columnNamePattern)
                    ) {
                        rows.add(
                            listOf(
                                VirtualSchema.CATALOG_NAME.ifEmpty { null },  // TABLE_CAT
                                VirtualSchema.SCHEMA_NAME,                     // TABLE_SCHEM
                                table.name,                                    // TABLE_NAME
                                col.name,                                      // COLUMN_NAME
                                col.sqlType,                                   // DATA_TYPE
                                col.typeName,                                  // TYPE_NAME
                                col.precision,                                 // COLUMN_SIZE
                                null,                                          // BUFFER_LENGTH
                                col.scale,                                     // DECIMAL_DIGITS
                                10,                                            // NUM_PREC_RADIX
                                if (col.nullable) DatabaseMetaData.columnNullable else DatabaseMetaData.columnNoNulls,
                                col.description,                               // REMARKS
                                null,                                          // COLUMN_DEF
                                null,                                          // SQL_DATA_TYPE
                                null,                                          // SQL_DATETIME_SUB
                                if (col.typeName == "VARCHAR") col.precision else null, // CHAR_OCTET_LENGTH
                                index + 1,                                     // ORDINAL_POSITION
                                if (col.nullable) "YES" else "NO",             // IS_NULLABLE
                                null,                                          // SCOPE_CATALOG
                                null,                                          // SCOPE_SCHEMA
                                null,                                          // SCOPE_TABLE
                                null,                                          // SOURCE_DATA_TYPE
                                "NO",                                          // IS_AUTOINCREMENT
                                "NO"                                           // IS_GENERATEDCOLUMN
                            )
                        )
                    }
                }
            }

        return SimpleResultSet(columns, rows)
    }

    /**
     * Get primary keys for the specified table.
     */
    override fun getPrimaryKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        val columns = listOf(
            ResultSetColumn("TABLE_CAT", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_SCHEM", Types.VARCHAR, 128),
            ResultSetColumn("TABLE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("COLUMN_NAME", Types.VARCHAR, 128),
            ResultSetColumn("KEY_SEQ", Types.SMALLINT, 5),
            ResultSetColumn("PK_NAME", Types.VARCHAR, 128)
        )

        if (table == null) {
            return SimpleResultSet(columns, emptyList())
        }

        val tableDef = VirtualSchema.getTable(table)
            ?: return SimpleResultSet(columns, emptyList())

        val rows = tableDef.primaryKey.mapIndexed { index, colName ->
            listOf(
                VirtualSchema.CATALOG_NAME.ifEmpty { null },
                VirtualSchema.SCHEMA_NAME,
                tableDef.name,
                colName,
                (index + 1).toShort(),
                "PK_${tableDef.name.uppercase()}"
            )
        }

        return SimpleResultSet(columns, rows)
    }

    // ========== Feature Support ==========

    override fun isReadOnly(): Boolean = true

    override fun supportsTransactions(): Boolean = false

    override fun supportsTransactionIsolationLevel(level: Int): Boolean =
        level == Connection.TRANSACTION_NONE || level == Connection.TRANSACTION_READ_COMMITTED

    override fun supportsDataDefinitionAndDataManipulationTransactions(): Boolean = false

    override fun supportsDataManipulationTransactionsOnly(): Boolean = false

    override fun dataDefinitionCausesTransactionCommit(): Boolean = false

    override fun dataDefinitionIgnoredInTransactions(): Boolean = true

    override fun supportsResultSetType(type: Int): Boolean =
        type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE

    override fun supportsResultSetConcurrency(type: Int, concurrency: Int): Boolean =
        concurrency == ResultSet.CONCUR_READ_ONLY

    override fun ownUpdatesAreVisible(type: Int): Boolean = false

    override fun ownDeletesAreVisible(type: Int): Boolean = false

    override fun ownInsertsAreVisible(type: Int): Boolean = false

    override fun othersUpdatesAreVisible(type: Int): Boolean = false

    override fun othersDeletesAreVisible(type: Int): Boolean = false

    override fun othersInsertsAreVisible(type: Int): Boolean = false

    override fun updatesAreDetected(type: Int): Boolean = false

    override fun deletesAreDetected(type: Int): Boolean = false

    override fun insertsAreDetected(type: Int): Boolean = false

    override fun supportsBatchUpdates(): Boolean = false

    override fun supportsResultSetHoldability(holdability: Int): Boolean = true

    override fun getResultSetHoldability(): Int = ResultSet.CLOSE_CURSORS_AT_COMMIT

    override fun getSQLStateType(): Int = DatabaseMetaData.sqlStateSQL

    override fun locatorsUpdateCopy(): Boolean = false

    override fun supportsStatementPooling(): Boolean = false

    override fun getRowIdLifetime(): RowIdLifetime = RowIdLifetime.ROWID_UNSUPPORTED

    override fun supportsStoredFunctionsUsingCallSyntax(): Boolean = false

    override fun autoCommitFailureClosesAllResultSets(): Boolean = false

    override fun supportsSavepoints(): Boolean = false

    override fun supportsNamedParameters(): Boolean = false

    override fun supportsMultipleOpenResults(): Boolean = false

    override fun supportsGetGeneratedKeys(): Boolean = false

    override fun generatedKeyAlwaysReturned(): Boolean = false

    // ========== SQL Support ==========

    override fun supportsAlterTableWithAddColumn(): Boolean = false

    override fun supportsAlterTableWithDropColumn(): Boolean = false

    override fun supportsColumnAliasing(): Boolean = true

    override fun nullPlusNonNullIsNull(): Boolean = true

    override fun supportsConvert(): Boolean = false

    override fun supportsConvert(fromType: Int, toType: Int): Boolean = false

    override fun supportsTableCorrelationNames(): Boolean = true

    override fun supportsDifferentTableCorrelationNames(): Boolean = false

    override fun supportsExpressionsInOrderBy(): Boolean = true

    override fun supportsOrderByUnrelated(): Boolean = true

    override fun supportsGroupBy(): Boolean = true

    override fun supportsGroupByUnrelated(): Boolean = true

    override fun supportsGroupByBeyondSelect(): Boolean = true

    override fun supportsLikeEscapeClause(): Boolean = true

    override fun supportsMultipleResultSets(): Boolean = false

    override fun supportsMultipleTransactions(): Boolean = false

    override fun supportsNonNullableColumns(): Boolean = true

    override fun supportsMinimumSQLGrammar(): Boolean = true

    override fun supportsCoreSQLGrammar(): Boolean = false

    override fun supportsExtendedSQLGrammar(): Boolean = false

    override fun supportsANSI92EntryLevelSQL(): Boolean = false

    override fun supportsANSI92IntermediateSQL(): Boolean = false

    override fun supportsANSI92FullSQL(): Boolean = false

    override fun supportsIntegrityEnhancementFacility(): Boolean = false

    override fun supportsOuterJoins(): Boolean = false

    override fun supportsFullOuterJoins(): Boolean = false

    override fun supportsLimitedOuterJoins(): Boolean = false

    override fun getProcedureTerm(): String = "procedure"

    override fun supportsStoredProcedures(): Boolean = false

    override fun supportsSubqueriesInComparisons(): Boolean = false

    override fun supportsSubqueriesInExists(): Boolean = false

    override fun supportsSubqueriesInIns(): Boolean = false

    override fun supportsSubqueriesInQuantifieds(): Boolean = false

    override fun supportsCorrelatedSubqueries(): Boolean = false

    override fun supportsUnion(): Boolean = false

    override fun supportsUnionAll(): Boolean = false

    override fun supportsOpenCursorsAcrossCommit(): Boolean = false

    override fun supportsOpenCursorsAcrossRollback(): Boolean = false

    override fun supportsOpenStatementsAcrossCommit(): Boolean = true

    override fun supportsOpenStatementsAcrossRollback(): Boolean = true

    override fun supportsPositionedDelete(): Boolean = false

    override fun supportsPositionedUpdate(): Boolean = false

    override fun supportsSelectForUpdate(): Boolean = false

    override fun storesLowerCaseIdentifiers(): Boolean = true

    override fun storesLowerCaseQuotedIdentifiers(): Boolean = false

    override fun storesMixedCaseIdentifiers(): Boolean = false

    override fun storesMixedCaseQuotedIdentifiers(): Boolean = true

    override fun storesUpperCaseIdentifiers(): Boolean = false

    override fun storesUpperCaseQuotedIdentifiers(): Boolean = false

    override fun supportsMixedCaseIdentifiers(): Boolean = false

    override fun supportsMixedCaseQuotedIdentifiers(): Boolean = true

    override fun getIdentifierQuoteString(): String = "\""

    override fun getSQLKeywords(): String = ""

    override fun getNumericFunctions(): String = "ABS,FLOOR,CEIL,ROUND"

    override fun getStringFunctions(): String = "CONCAT,LOWER,UPPER,LENGTH,SUBSTRING"

    override fun getSystemFunctions(): String = ""

    override fun getTimeDateFunctions(): String = ""

    override fun getSearchStringEscape(): String = "\\"

    override fun getExtraNameCharacters(): String = ""

    // ========== Limits ==========

    override fun getMaxBinaryLiteralLength(): Int = 0

    override fun getMaxCharLiteralLength(): Int = 0

    override fun getMaxColumnNameLength(): Int = 128

    override fun getMaxColumnsInGroupBy(): Int = 0

    override fun getMaxColumnsInIndex(): Int = 0

    override fun getMaxColumnsInOrderBy(): Int = 0

    override fun getMaxColumnsInSelect(): Int = 0

    override fun getMaxColumnsInTable(): Int = 0

    override fun getMaxConnections(): Int = 0

    override fun getMaxCursorNameLength(): Int = 0

    override fun getMaxIndexLength(): Int = 0

    override fun getMaxSchemaNameLength(): Int = 128

    override fun getMaxProcedureNameLength(): Int = 0

    override fun getMaxCatalogNameLength(): Int = 0

    override fun getMaxRowSize(): Int = 0

    override fun doesMaxRowSizeIncludeBlobs(): Boolean = false

    override fun getMaxStatementLength(): Int = 0

    override fun getMaxStatements(): Int = 0

    override fun getMaxTableNameLength(): Int = 128

    override fun getMaxTablesInSelect(): Int = 1

    override fun getMaxUserNameLength(): Int = 0

    override fun getDefaultTransactionIsolation(): Int = Connection.TRANSACTION_READ_COMMITTED

    override fun nullsAreSortedHigh(): Boolean = false

    override fun nullsAreSortedLow(): Boolean = true

    override fun nullsAreSortedAtStart(): Boolean = false

    override fun nullsAreSortedAtEnd(): Boolean = false

    override fun usesLocalFiles(): Boolean = false

    override fun usesLocalFilePerTable(): Boolean = false

    override fun allProceduresAreCallable(): Boolean = false

    override fun allTablesAreSelectable(): Boolean = true

    // ========== Empty Result Sets for Unsupported Features ==========

    override fun getProcedures(catalog: String?, schemaPattern: String?, procedureNamePattern: String?): ResultSet {
        return SimpleResultSet(
            listOf(
                ResultSetColumn("PROCEDURE_CAT", Types.VARCHAR, 128),
                ResultSetColumn("PROCEDURE_SCHEM", Types.VARCHAR, 128),
                ResultSetColumn("PROCEDURE_NAME", Types.VARCHAR, 128),
                ResultSetColumn("reserved1", Types.VARCHAR, 128),
                ResultSetColumn("reserved2", Types.VARCHAR, 128),
                ResultSetColumn("reserved3", Types.VARCHAR, 128),
                ResultSetColumn("REMARKS", Types.VARCHAR, 512),
                ResultSetColumn("PROCEDURE_TYPE", Types.SMALLINT, 5),
                ResultSetColumn("SPECIFIC_NAME", Types.VARCHAR, 128)
            ),
            emptyList()
        )
    }

    override fun getProcedureColumns(
        catalog: String?,
        schemaPattern: String?,
        procedureNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getColumnPrivileges(
        catalog: String?,
        schema: String?,
        table: String?,
        columnNamePattern: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getTablePrivileges(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getBestRowIdentifier(
        catalog: String?,
        schema: String?,
        table: String?,
        scope: Int,
        nullable: Boolean
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getVersionColumns(catalog: String?, schema: String?, table: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getImportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getExportedKeys(catalog: String?, schema: String?, table: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getCrossReference(
        parentCatalog: String?,
        parentSchema: String?,
        parentTable: String?,
        foreignCatalog: String?,
        foreignSchema: String?,
        foreignTable: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getTypeInfo(): ResultSet {
        val columns = listOf(
            ResultSetColumn("TYPE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("DATA_TYPE", Types.INTEGER, 10),
            ResultSetColumn("PRECISION", Types.INTEGER, 10),
            ResultSetColumn("LITERAL_PREFIX", Types.VARCHAR, 32),
            ResultSetColumn("LITERAL_SUFFIX", Types.VARCHAR, 32),
            ResultSetColumn("CREATE_PARAMS", Types.VARCHAR, 128),
            ResultSetColumn("NULLABLE", Types.SMALLINT, 5),
            ResultSetColumn("CASE_SENSITIVE", Types.BOOLEAN, 1),
            ResultSetColumn("SEARCHABLE", Types.SMALLINT, 5),
            ResultSetColumn("UNSIGNED_ATTRIBUTE", Types.BOOLEAN, 1),
            ResultSetColumn("FIXED_PREC_SCALE", Types.BOOLEAN, 1),
            ResultSetColumn("AUTO_INCREMENT", Types.BOOLEAN, 1),
            ResultSetColumn("LOCAL_TYPE_NAME", Types.VARCHAR, 128),
            ResultSetColumn("MINIMUM_SCALE", Types.SMALLINT, 5),
            ResultSetColumn("MAXIMUM_SCALE", Types.SMALLINT, 5),
            ResultSetColumn("SQL_DATA_TYPE", Types.INTEGER, 10),
            ResultSetColumn("SQL_DATETIME_SUB", Types.INTEGER, 10),
            ResultSetColumn("NUM_PREC_RADIX", Types.INTEGER, 10)
        )

        val rows = listOf(
            listOf("VARCHAR", Types.VARCHAR, 65535, "'", "'", "length", 1.toShort(), true, 3.toShort(), false, false, false, "VARCHAR", 0.toShort(), 0.toShort(), null, null, 10),
            listOf("BIGINT", Types.BIGINT, 19, null, null, null, 1.toShort(), false, 3.toShort(), false, false, false, "BIGINT", 0.toShort(), 0.toShort(), null, null, 10),
            listOf("INTEGER", Types.INTEGER, 10, null, null, null, 1.toShort(), false, 3.toShort(), false, false, false, "INTEGER", 0.toShort(), 0.toShort(), null, null, 10),
            listOf("DECIMAL", Types.DECIMAL, 78, null, null, "precision,scale", 1.toShort(), false, 3.toShort(), false, false, false, "DECIMAL", 0.toShort(), 0.toShort(), null, null, 10),
            listOf("BOOLEAN", Types.BOOLEAN, 1, null, null, null, 1.toShort(), false, 3.toShort(), false, false, false, "BOOLEAN", 0.toShort(), 0.toShort(), null, null, 10),
            listOf("TIMESTAMP WITH TIME ZONE", Types.TIMESTAMP_WITH_TIMEZONE, 26, "'", "'", null, 1.toShort(), false, 3.toShort(), false, false, false, "TIMESTAMP WITH TIME ZONE", 0.toShort(), 6.toShort(), null, null, 10)
        )

        return SimpleResultSet(columns, rows)
    }

    override fun getIndexInfo(
        catalog: String?,
        schema: String?,
        table: String?,
        unique: Boolean,
        approximate: Boolean
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getUDTs(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        types: IntArray?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getSuperTypes(catalog: String?, schemaPattern: String?, typeNamePattern: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getSuperTables(catalog: String?, schemaPattern: String?, tableNamePattern: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getAttributes(
        catalog: String?,
        schemaPattern: String?,
        typeNamePattern: String?,
        attributeNamePattern: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getClientInfoProperties(): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getFunctions(catalog: String?, schemaPattern: String?, functionNamePattern: String?): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getFunctionColumns(
        catalog: String?,
        schemaPattern: String?,
        functionNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

    override fun getPseudoColumns(
        catalog: String?,
        schemaPattern: String?,
        tableNamePattern: String?,
        columnNamePattern: String?
    ): ResultSet {
        return SimpleResultSet(emptyList(), emptyList())
    }

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

    // ========== Helper Functions ==========

    /**
     * Match a name against a SQL LIKE pattern.
     */
    private fun matchesPattern(name: String, pattern: String): Boolean {
        val regex = pattern
            .replace("%", ".*")
            .replace("_", ".")
        return name.matches(Regex(regex, RegexOption.IGNORE_CASE))
    }
}

/**
 * Column definition for SimpleResultSet.
 */
data class ResultSetColumn(
    val name: String,
    val sqlType: Int,
    val precision: Int
)
