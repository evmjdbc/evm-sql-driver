# EVM JDBC Driver

A JDBC driver that lets you query EVM-compatible blockchains (Ethereum, Avalanche, Polygon, Arbitrum, etc.) using standard SQL. Use your favorite database tools like DataGrip, DBeaver, or any JDBC-compatible application to explore blockchain data.

## Features

- **Standard JDBC 4.2+** - Works with any JDBC-compatible tool
- **SQL Query Support** - SELECT, WHERE, ORDER BY, LIMIT, DISTINCT
- **Advanced SQL** - JOINs, UNIONs, subqueries, CTEs (WITH clause)
- **Arithmetic Expressions** - `SELECT balance / 1e18 AS balance_eth`
- **Multiple Tables** - blocks, transactions, logs, accounts, erc20_transfers, erc721_transfers
- **Any EVM Chain** - Ethereum, Avalanche, Polygon, Arbitrum, Base, and more

## Installation

### Maven

```xml
<dependency>
    <groupId>com.evmjdbc</groupId>
    <artifactId>evm-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```kotlin
implementation("com.evmjdbc:evm-jdbc-driver:1.0.0")
```

### Manual (DataGrip, DBeaver, etc.)

Download the JAR from [Maven Central](https://central.sonatype.com/artifact/com.evmjdbc/evm-jdbc-driver) and add it as a driver in your database tool.

## Connection URL

```
jdbc:evm:<rpc_url>
```

### Examples

```
# Ethereum Mainnet (via public RPC)
jdbc:evm:https://eth.llamarpc.com

# Avalanche C-Chain
jdbc:evm:https://api.avax.network/ext/bc/C/rpc

# Polygon
jdbc:evm:https://polygon-rpc.com

# Arbitrum
jdbc:evm:https://arb1.arbitrum.io/rpc

# Base
jdbc:evm:https://mainnet.base.org
```

## Supported Tables

| Table | Description | Required Filter |
|-------|-------------|-----------------|
| `blocks` | Block headers | None (scans by range) |
| `transactions` | Transaction data with receipts | None or `hash`/`block_number` |
| `logs` | Event logs | `block_number` range recommended |
| `accounts` | Account balances and code | `address` (required) |
| `erc20_transfers` | ERC-20 Transfer events | `block_number` range |
| `erc721_transfers` | ERC-721 Transfer events | `block_number` range |
| `chain_info` | Chain metadata (chain_id, gas_price) | None |
| `rpc_methods` | Supported RPC methods | None |

## Query Examples

### Basic Queries

```sql
-- Get latest 10 blocks
SELECT * FROM blocks ORDER BY number DESC LIMIT 10;

-- Get specific block
SELECT * FROM blocks WHERE number = 18000000;

-- Get transaction by hash
SELECT * FROM transactions
WHERE hash = '0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060';
```

### Account Queries

```sql
-- Get account balance (requires address filter)
SELECT address, balance, balance / 1e18 AS balance_eth
FROM accounts
WHERE address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045';

-- Multiple accounts
SELECT * FROM accounts
WHERE address IN (
    '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045',
    '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8'
);
```

### Transaction Queries

```sql
-- Transactions in a block
SELECT hash, from_address, to_address, value / 1e18 AS value_eth
FROM transactions
WHERE block_number = 18000000;

-- Filter by sender
SELECT * FROM transactions
WHERE block_number = 18000000
  AND from_address = '0x1234...';
```

### Event Log Queries

```sql
-- Get logs in block range
SELECT * FROM logs
WHERE block_number BETWEEN 18000000 AND 18000100;

-- Filter by contract address
SELECT * FROM logs
WHERE block_number >= 18000000
  AND block_number <= 18000100
  AND address = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48';
```

### ERC-20 Transfers

```sql
-- Get USDC transfers in a block range
SELECT from_address, to_address, amount / 1e6 AS usdc_amount
FROM erc20_transfers
WHERE block_number BETWEEN 18000000 AND 18000100
  AND token_address = '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48';
```

### JOINs

```sql
-- Join transactions with blocks
SELECT t.hash, t.value, b.timestamp
FROM transactions t
JOIN blocks b ON t.block_number = b.number
WHERE t.block_number = 18000000;
```

### UNIONs

```sql
-- Combine results from multiple queries
SELECT address, balance FROM accounts WHERE address = '0x1234...'
UNION ALL
SELECT address, balance FROM accounts WHERE address = '0x5678...';
```

### Subqueries

```sql
-- Use subquery as data source
SELECT * FROM (
    SELECT address, balance / 1e18 AS balance_eth
    FROM accounts
    WHERE address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045'
) AS whale_accounts;
```

### CTEs (Common Table Expressions)

```sql
-- Use WITH clause for complex queries
WITH my_accounts AS (
    SELECT address, balance / 1e18 AS balance_eth
    FROM accounts
    WHERE address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045'
)
SELECT * FROM my_accounts WHERE balance_eth > 100;
```

### Arithmetic Expressions

```sql
-- Convert wei to ETH
SELECT
    address,
    balance,
    balance / 1e18 AS balance_eth,
    balance / 1e18 * 2000 AS balance_usd  -- Assuming $2000/ETH
FROM accounts
WHERE address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045';
```

## Java Usage

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:evm:https://eth.llamarpc.com";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT number, hash, timestamp FROM blocks LIMIT 5")) {

            while (rs.next()) {
                System.out.printf("Block %d: %s%n",
                    rs.getLong("number"),
                    rs.getString("hash"));
            }
        }
    }
}
```

## Kotlin Usage

```kotlin
import java.sql.DriverManager

fun main() {
    val url = "jdbc:evm:https://eth.llamarpc.com"

    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT * FROM blocks LIMIT 5").use { rs ->
                while (rs.next()) {
                    println("Block ${rs.getLong("number")}: ${rs.getString("hash")}")
                }
            }
        }
    }
}
```

## Limitations

- **Read-only** - No INSERT, UPDATE, DELETE (use `eth_sendRawTransaction` directly for writes)
- **No aggregations** - COUNT, SUM, AVG not yet supported
- **Rate limits** - Public RPCs may rate limit; use private RPC for heavy queries

## License

MIT License - see [LICENSE](LICENSE) for details.
