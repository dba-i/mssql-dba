# MSSQL DBA MCP Stdio Server

The server provides a functionality to handle DBA tasks for SQL Server database, such as analyzing tables, assesing index health, and finding missing indices. Many more tasks are upcoming. **Currently we support only SQL Server Authenitcation** method. Please join our [Discord](https://discord.gg/Fs3Nqpgx) and share your feedback!

## Prerequisites
Create a user with these permissions:

```sql
USE [master];
GO

CREATE LOGIN [dbai] WITH PASSWORD = 'YourStrongPasswordHere';
GO

GRANT VIEW SERVER STATE TO [dbai];
GO

USE [YourDatabase];
GO

CREATE USER [dbai] FOR LOGIN [dbai];
GO

GRANT VIEW DEFINITION TO [dbai];
GO
```

## Features

- **Table Metadata Retrieval**
- **Index Health Assessment**
- **Missing Index Detection**
- **Query Optimization Prompt**

## Configuration

### Usage with MCP Client

There is a partial list of existing clients at [modelcontextprotocol.io](https://modelcontextprotocol.io/clients). Consult their documentation to install the MCP server.

**Example setup for Claude Code:** Add this to your `claude_desktop_config.json`:

#### npx

```json
{
  "mcpServers": {
    "mssql-dba": {
      "command": "npx",
      "args": ["mssql-dba"],
      "env": {
        // Required Parameters
        "DB_HOST": "host",
        "DB_PORT": "1433",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        "DB_NAME": "database name",
        // Optional Parameters
        "TRUST_SERVER_CERTIFICATE": "true",
        "ENCRYPT": "false",
        "MAX_POOL": "10",
        "MIN_POOL": "0",
        "IDLE": "30000"
      }
    }
  }
}
```

## Prompts

### 1. Optimize Query

- **Name:** `optimize-query`
- **Description:** Optimize SQL queries for better performance.
- **Input:**
  - `query` (string): The SQL query to optimize.
- **Behavior:**
  - Fetches schema information for tables involved in the query.
  - Suggests schema-level optimizations and highlights query inefficiencies.
  - Provides step-by-step guidance for improvements, prioritizing schema-level changes.

## Tools

The MCP server exposes the following tools:

### 1. Get Tables Info

- **Name:** `get-tables-info`
- **Description:** Get the metadata about specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to retrieve metadata for.
- **Output:**
  - JSON metadata about the specified tables.

### 2. Get Tables Index Health

- **Name:** `get-tables-index-health`
- **Description:** Assess index health for specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to assess.
- **Output:**
  - JSON with index health information for the specified tables.

### 3. Get Tables Missing Indices

- **Name:** `get-tables-missing-indices`
- **Description:** Identify missing indices for specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to check for missing indices.
- **Output:**
  - JSON with missing indices and suggested `CREATE INDEX` statements.

## License

This project is licensed under the Apache License 2.0.

## Contact

For questions or support, please contact the maintainer.
