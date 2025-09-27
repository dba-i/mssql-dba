# MSSQL DBA MCP Server - AI Coding Guidelines

## Architecture Overview

This is a **Model Context Protocol (MCP) server** that provides SQL Server database administration tools to LLMs. The server uses stdio transport and exposes database analysis tools as MCP tools and prompts.

### Core Components

- **`src/stdioServer.ts`**: Main MCP server setup with tool/prompt registration
- **`src/MSSQL.ts`**: Database connection wrapper with query execution and connection pooling
- **`src/index.ts`**: Entry point that establishes stdio transport
- **`src/features/`**: Organized by scope (schema-level vs table-level tools)

### Feature Organization Pattern

```
src/features/informationTools/
├── schemaLevelTools/     # Database-wide analysis (getActiveTablesInfo)
└── tableLevelTools/      # Specific table analysis (getTablesInfo, getTablesIndexHealth, getTablesMissingIndices)
```

## Development Patterns

### Database Tool Implementation

All database tools follow this pattern:

```typescript
export async function toolName({
  tableNames?, // Optional - table-level tools only
  db,
}: {
  tableNames?: string[];
  db: MSSQL;
}): Promise<string> {
  // Validation
  // SQL query construction with complex CTEs
  // Return JSON string for LLM consumption
}
```

### MCP Tool Registration

Tools are registered in `stdioServer.ts` with:

- Zod schema validation for inputs (`inputSchema: { tableNames: z.array(z.string()) }`)
- JSON string responses wrapped in MCP content format
- Descriptive context for LLM consumption

### SQL Query Architecture

- **Complex CTEs**: All queries use multiple Common Table Expressions for data aggregation
- **Performance metrics**: Focus on reads/writes ratios, index usage statistics
- **Human-readable output**: Queries include extensive comments explaining business logic
- **Parameterized filtering**: Table-specific tools use `IN` clauses for multi-table analysis

## Environment Configuration

Required environment variables (see README.md for full setup):

```bash
DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME  # Required
TRUST_SERVER_CERTIFICATE, ENCRYPT, MAX_POOL, MIN_POOL, IDLE  # Optional
```

Database user requires specific SQL Server permissions:

- `VIEW SERVER STATE` (server-level)
- `VIEW DEFINITION` (database-level)

## Build & Development

- **Build**: `npm run build` (TypeScript compilation with chmod for executable)
- **Module system**: ES modules (`"type": "module"` in package.json)
- **TypeScript config**: Node16 module resolution, strict mode, declaration maps
- **Testing**: Test files exist but are currently empty - tests should focus on SQL query correctness

## Key Implementation Details

### Database Connection Management

- Single connection pool instantiated at server startup
- Connection validation with `ensureConnected()` before queries
- Async/await pattern throughout with proper error handling

### MCP-Specific Conventions

- All tool responses include contextual text for LLM understanding
- Prompts (like `optimize-query`) provide step-by-step guidance emphasis
- Tools return analysis data; prompts return instruction messages

### SQL Server Specialization

- Queries target `sys.` catalog views and DMVs (Dynamic Management Views)
- Focus on index health, missing index detection, and table workload analysis
- Optimized for SQL Server authentication (Windows auth not supported)

## When Adding New Features

1. **Schema vs Table Level**: Determine if the tool analyzes entire database or specific tables
2. **Feature Directory**: Place in appropriate `informationTools` subdirectory
3. **Export Pattern**: Export from feature file, import in `stdioServer.ts`
4. **SQL Complexity**: Expect to write complex CTEs with performance-focused analysis
5. **LLM Context**: Always return human-readable JSON with explanatory context
