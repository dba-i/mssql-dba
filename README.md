# MSSQL DBA MCP Stdio Server

The server provides context to an LLM, empowering models to complete various Database Administration tasks, such as improving table health, optimizing existing indices, and identifying missing ones. The server aims to find the best way to provide context so the LLMs can maximize their potential in completing DBA tasks. For feedback, questions, or support, please join our [Discord](https://discord.gg/G9GGkrykBj)!

## Table of Contents

- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
  - [Usage with MCP Client](#usage-with-mcp-client)
    - [npx](#npx)
- [Prompts](#prompts)
  - [Optimize Query](#optimize-query)
  - [Optimize Indices](#optimize-indices)
- [Tools](#tools)
  - [Table-Level Tools](#table-level-tools)
    - [Get Tables Info](#get-tables-info)
    - [Get Tables Index Health](#get-tables-index-health)
    - [Get Tables Missing Indices](#get-tables-missing-indices)
  - [Server-Level Tools](#server-level-tools)
    - [Get Server Info](#get-server-info)
  - [Database-Level Tools](#database-level-tools)
    - [Get Database Collation](#get-database-collation)
    - [Get Collation Mismatches](#get-collation-mismatches)
- [License](#license)
- [Contact](#contact)

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

## Configuration

### Usage with MCP Client

There is a partial list of existing clients at [modelcontextprotocol.io](https://modelcontextprotocol.io/clients). Consult their documentation to install the MCP server.

**Currently we support only SQL Server Authenitcation** method.

**Example setup for Claude Code:** Add this to your `claude_desktop_config.json`:

#### npx

```json
{
  "mcpServers": {
    "mssql-dba": {
      "command": "npx",
      "args": ["@dba-i/mssql-dba"],
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

### Optimize Query

- **Name:** `optimize-query`
- **Description:** Optimize SQL queries for better performance.
- **Input:**
  - `query`: The SQL query to optimize.
- **Behavior:**
  - Identifies tables involved in the query and fetches comprehensive schema information.
  - Analyzes existing indices and identifies potential redundancies or missing indices.
  - Suggests schema-level optimizations and highlights query inefficiencies.
  - Generates optimized query files and schema optimization scripts with detailed documentation.

### Optimize Indices

- **Name:** `optimize-indices`
- **Description:** Optimize indices on specified tables.
- **Input:**
  - `tableNames`: A string containing the names of tables to optimize.
- **Behavior:**
  - Fetches information about existing and missing indices for the specified tables.
  - Identifies inefficient indices and suggests removing them.
  - Provides options for index consolidation where possible.
  - Analyzes missing indices and includes them if necessary, while avoiding redundant indices.
  - Generates index optimization scripts with detailed documentation.
  - Creates rollback scripts and index maintenance stored procedures.

## Tools

The MCP server exposes the following tools organized by scope:

### Table-Level Tools

#### Get Tables Info

- **Name:** `get-tables-info`
- **Description:** Get the metadata about specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to retrieve metadata for.
- **Output:**
  - JSON metadata about the specified tables.

#### Get Tables Index Health

- **Name:** `get-tables-index-health`
- **Description:** Assess index health for specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to assess.
- **Output:**
  - JSON with index health information for the specified tables.

#### Get Tables Missing Indices

- **Name:** `get-tables-missing-indices`
- **Description:** Identify missing indices for specified tables.
- **Input:**
  - `tableNames` (array of strings): Names of the tables to check for missing indices.
- **Output:**
  - JSON with missing indices.

### Server-Level Tools

#### Get Server Info

- **Name:** `get-server-info`
- **Description:** Retrieve information about the SQL Server instance such as version, current update level, edition, and licensing details.
- **Input:**
  - No input parameters required.
- **Output:**
  - JSON with comprehensive SQL Server instance information.

### Database-Level Tools

#### Get Database Collation

- **Name:** `get-db-collation`
- **Description:** Retrieve the collation setting for the current database.
- **Input:**
  - No input parameters required.
- **Output:**
  - JSON with the database's collation information.

#### Get Collation Mismatches

- **Name:** `get-collation-mismatches`
- **Description:** Retrieve the columns with collation settings that differ from the database default.
- **Input:**
  - No input parameters required.
- **Output:**
  - JSON with information about columns that have collation mismatches with the database default.

## License

This project is licensed under the Apache License 2.0.

## Contact

For feedback, questions or support, please join our [Discord](https://discord.gg/G9GGkrykBj).
