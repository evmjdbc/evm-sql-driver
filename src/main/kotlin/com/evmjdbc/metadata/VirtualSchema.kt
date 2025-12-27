package com.evmjdbc.metadata

import java.sql.Types

/**
 * Defines the virtual SQL schema for EVM blockchain data.
 */
object VirtualSchema {
    const val SCHEMA_NAME = "evm"
    const val CATALOG_NAME = ""

    /**
     * Table type constants.
     */
    object TableTypes {
        const val TABLE = "TABLE"
        const val VIEW = "VIEW"
        const val SYSTEM_TABLE = "SYSTEM TABLE"
    }

    /**
     * Column definition.
     */
    data class ColumnDef(
        val name: String,
        val sqlType: Int,
        val typeName: String,
        val precision: Int,
        val scale: Int = 0,
        val nullable: Boolean,
        val description: String = ""
    )

    /**
     * Table definition.
     */
    data class TableDef(
        val name: String,
        val type: String,
        val columns: List<ColumnDef>,
        val primaryKey: List<String>,
        val description: String = ""
    )

    // ========== Column Definitions by Type ==========

    private val bigintColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.BIGINT, "BIGINT", 19, 0, nullable, desc)
    }

    private val intColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.INTEGER, "INTEGER", 10, 0, nullable, desc)
    }

    private val decimalColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.DECIMAL, "DECIMAL", 78, 0, nullable, desc)
    }

    private val addressColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.VARCHAR, "VARCHAR", 42, 0, nullable, desc)
    }

    private val hashColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.VARCHAR, "VARCHAR", 66, 0, nullable, desc)
    }

    private val varcharColumn = { name: String, size: Int, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.VARCHAR, "VARCHAR", size, 0, nullable, desc)
    }

    private val timestampColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.TIMESTAMP_WITH_TIMEZONE, "TIMESTAMP WITH TIME ZONE", 26, 0, nullable, desc)
    }

    private val booleanColumn = { name: String, nullable: Boolean, desc: String ->
        ColumnDef(name, Types.BOOLEAN, "BOOLEAN", 1, 0, nullable, desc)
    }

    // ========== Table Definitions ==========

    val BLOCKS = TableDef(
        name = "blocks",
        type = TableTypes.TABLE,
        primaryKey = listOf("number"),
        description = "Ethereum block headers",
        columns = listOf(
            bigintColumn("number", false, "Block number (height)"),
            hashColumn("hash", false, "Block hash with 0x prefix"),
            hashColumn("parent_hash", false, "Parent block hash"),
            varcharColumn("nonce", 18, true, "Block nonce (PoW only)"),
            hashColumn("sha3_uncles", false, "Uncles hash"),
            varcharColumn("logs_bloom", 514, false, "Bloom filter for logs"),
            hashColumn("transactions_root", false, "Transactions trie root"),
            hashColumn("state_root", false, "State trie root"),
            hashColumn("receipts_root", false, "Receipts trie root"),
            addressColumn("miner", false, "Block producer address"),
            decimalColumn("difficulty", false, "Block difficulty (PoW)"),
            decimalColumn("total_difficulty", true, "Chain total difficulty"),
            varcharColumn("extra_data", 1024, false, "Extra data field (hex)"),
            bigintColumn("size", false, "Block size in bytes"),
            bigintColumn("gas_limit", false, "Gas limit"),
            bigintColumn("gas_used", false, "Gas used"),
            timestampColumn("timestamp", false, "Block timestamp"),
            intColumn("transaction_count", false, "Number of transactions"),
            decimalColumn("base_fee_per_gas", true, "EIP-1559 base fee (null pre-London)"),
            hashColumn("withdrawals_root", true, "Withdrawals trie root (post-Shanghai)")
        )
    )

    val TRANSACTIONS = TableDef(
        name = "transactions",
        type = TableTypes.TABLE,
        primaryKey = listOf("hash"),
        description = "Blockchain transactions with receipt data",
        columns = listOf(
            hashColumn("hash", false, "Transaction hash"),
            bigintColumn("block_number", false, "Block containing this tx"),
            hashColumn("block_hash", false, "Block hash"),
            intColumn("transaction_index", false, "Position in block"),
            addressColumn("from_address", false, "Sender address (checksummed)"),
            addressColumn("to_address", true, "Recipient address (null for contract creation)"),
            decimalColumn("value", false, "Value in wei"),
            bigintColumn("gas", false, "Gas limit provided"),
            decimalColumn("gas_price", true, "Gas price (legacy tx)"),
            decimalColumn("max_fee_per_gas", true, "Max fee per gas (EIP-1559)"),
            decimalColumn("max_priority_fee_per_gas", true, "Max priority fee (EIP-1559)"),
            varcharColumn("input", 65535, false, "Input data (hex), respects inputDataMode"),
            bigintColumn("input_size", false, "Input data size in bytes"),
            bigintColumn("nonce", false, "Sender nonce"),
            intColumn("type", false, "Transaction type (0=legacy, 1=2930, 2=1559)"),
            bigintColumn("chain_id", true, "Chain ID (EIP-155)"),
            bigintColumn("v", false, "Signature v"),
            hashColumn("r", false, "Signature r"),
            hashColumn("s", false, "Signature s"),
            intColumn("status", false, "Receipt status (1=success, 0=failure)"),
            bigintColumn("gas_used", false, "Actual gas used (from receipt)"),
            decimalColumn("effective_gas_price", false, "Actual gas price paid (from receipt)"),
            addressColumn("contract_address", true, "Created contract address (if contract creation)"),
            intColumn("logs_count", false, "Number of logs emitted")
        )
    )

    val LOGS = TableDef(
        name = "logs",
        type = TableTypes.TABLE,
        primaryKey = listOf("block_number", "log_index"),
        description = "Event logs emitted by smart contracts",
        columns = listOf(
            bigintColumn("block_number", false, "Block number"),
            hashColumn("block_hash", false, "Block hash"),
            hashColumn("transaction_hash", false, "Transaction hash"),
            intColumn("transaction_index", false, "Transaction position in block"),
            intColumn("log_index", false, "Log position in block"),
            addressColumn("address", false, "Contract address emitting log"),
            hashColumn("topic0", true, "Event signature hash"),
            hashColumn("topic1", true, "First indexed parameter"),
            hashColumn("topic2", true, "Second indexed parameter"),
            hashColumn("topic3", true, "Third indexed parameter"),
            varcharColumn("data", 65535, false, "Non-indexed parameters (hex)"),
            booleanColumn("removed", false, "True if log was removed (chain reorg)")
        )
    )

    val ACCOUNTS = TableDef(
        name = "accounts",
        type = TableTypes.TABLE,
        primaryKey = listOf("address"),
        description = "Point-in-time account state",
        columns = listOf(
            addressColumn("address", false, "Account address (checksummed)"),
            decimalColumn("balance", false, "Balance in wei"),
            bigintColumn("nonce", false, "Transaction count"),
            hashColumn("code_hash", false, "Code hash (EMPTY_CODE_HASH if EOA)"),
            booleanColumn("is_contract", false, "True if account has code")
        )
    )

    val ERC20_TRANSFERS = TableDef(
        name = "erc20_transfers",
        type = TableTypes.VIEW,
        primaryKey = listOf("block_number", "log_index"),
        description = "Decoded ERC20 Transfer events",
        columns = listOf(
            bigintColumn("block_number", false, "Block number"),
            intColumn("log_index", false, "Log position in block"),
            hashColumn("transaction_hash", false, "Transaction hash"),
            addressColumn("token_address", false, "ERC20 contract address"),
            addressColumn("from_address", false, "Sender address"),
            addressColumn("to_address", false, "Recipient address"),
            decimalColumn("amount", false, "Token amount (raw, not adjusted for decimals)")
        )
    )

    val ERC20_BALANCES = TableDef(
        name = "erc20_balances",
        type = TableTypes.VIEW,
        primaryKey = listOf("token_address", "holder_address"),
        description = "Current ERC20 token balances (requires eth_call)",
        columns = listOf(
            addressColumn("token_address", false, "ERC20 contract address"),
            addressColumn("holder_address", false, "Token holder address"),
            decimalColumn("balance", false, "Token balance (raw)")
        )
    )

    val ERC721_TRANSFERS = TableDef(
        name = "erc721_transfers",
        type = TableTypes.VIEW,
        primaryKey = listOf("block_number", "log_index"),
        description = "Decoded ERC721 Transfer events",
        columns = listOf(
            bigintColumn("block_number", false, "Block number"),
            intColumn("log_index", false, "Log position in block"),
            hashColumn("transaction_hash", false, "Transaction hash"),
            addressColumn("token_address", false, "ERC721 contract address"),
            addressColumn("from_address", false, "Previous owner address"),
            addressColumn("to_address", false, "New owner address"),
            decimalColumn("token_id", false, "NFT token ID")
        )
    )

    val ERC721_OWNERS = TableDef(
        name = "erc721_owners",
        type = TableTypes.VIEW,
        primaryKey = listOf("token_address", "token_id"),
        description = "Current NFT ownership (requires eth_call)",
        columns = listOf(
            addressColumn("token_address", false, "ERC721 contract address"),
            decimalColumn("token_id", false, "NFT token ID"),
            addressColumn("owner_address", false, "Current owner address")
        )
    )

    val CHAIN_INFO = TableDef(
        name = "chain_info",
        type = TableTypes.SYSTEM_TABLE,
        primaryKey = listOf("chain_id"),
        description = "Chain metadata (singleton row)",
        columns = listOf(
            bigintColumn("chain_id", false, "EIP-155 chain ID"),
            varcharColumn("network_name", 100, false, "Human-readable network name"),
            bigintColumn("latest_block", false, "Current block number"),
            decimalColumn("gas_price", false, "Current gas price (wei)"),
            varcharColumn("client_version", 200, true, "RPC client version string")
        )
    )

    val RPC_METHODS = TableDef(
        name = "rpc_methods",
        type = TableTypes.SYSTEM_TABLE,
        primaryKey = listOf("method_name"),
        description = "Available RPC methods (informational)",
        columns = listOf(
            varcharColumn("method_name", 100, false, "RPC method name"),
            booleanColumn("supported", false, "True if driver supports this method"),
            varcharColumn("description", 500, true, "Method description")
        )
    )

    /**
     * All tables in the schema.
     */
    val ALL_TABLES: List<TableDef> = listOf(
        BLOCKS,
        TRANSACTIONS,
        LOGS,
        ACCOUNTS,
        ERC20_TRANSFERS,
        ERC20_BALANCES,
        ERC721_TRANSFERS,
        ERC721_OWNERS,
        CHAIN_INFO,
        RPC_METHODS
    )

    /**
     * Get table by name (case-insensitive).
     */
    fun getTable(name: String): TableDef? =
        ALL_TABLES.find { it.name.equals(name, ignoreCase = true) }

    /**
     * Check if table exists.
     */
    fun hasTable(name: String): Boolean = getTable(name) != null

    /**
     * Get column by table and column name.
     */
    fun getColumn(tableName: String, columnName: String): ColumnDef? =
        getTable(tableName)?.columns?.find { it.name.equals(columnName, ignoreCase = true) }
}
