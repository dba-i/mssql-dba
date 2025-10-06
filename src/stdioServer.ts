import { getTablesMissingIndices } from './features/informationTools/tableLevelTools/getTablesMissingIndices.js';
import { getCollationMismatches } from './features/informationTools/dbLevelTools/getCollationMismatches.js';
import { getTablesIndexHealth } from './features/informationTools/tableLevelTools/getTablesIndexHealth.js';
import { getServerInfo } from './features/informationTools/serverLevelTools/getServerInfo.js';
import { getTablesInfo } from './features/informationTools/tableLevelTools/getTablesInfo.js';
import { getDbCollation } from './features/informationTools/dbLevelTools/getDbCollation.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { MSSQL, MSSQLConfig } from './MSSQL.js';
import { z } from 'zod';
// Create server instance
export const mcpServer = new McpServer({
  name: 'mssql-dba',
  version: '1.0.0',
});

if (
  !process.env.DB_USER ||
  !process.env.DB_NAME ||
  !process.env.DB_PASSWORD ||
  !process.env.DB_HOST
) {
  throw new Error('Missing required database environment variables.');
}

const mssqlConfig: MSSQLConfig = {
  user: process.env.DB_USER,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_HOST,
  options: {
    trustServerCertificate: process.env.TRUST_SERVER_CERTIFICATE === 'true',
    encrypt: process.env.ENCRYPT === 'true',
  },
  pool: {
    max: process.env.MAX_POOL ? parseInt(process.env.MAX_POOL) : 10,
    min: process.env.MIN_POOL ? parseInt(process.env.MIN_POOL) : 0,
    idleTimeoutMillis: process.env.IDLE ? parseInt(process.env.IDLE) : 30000,
  },
};
const db = MSSQL.getInstance(mssqlConfig);

/* Prompts
--------------------------------------------------*/
mcpServer.registerPrompt(
  'optimize-query',
  {
    title: 'Optimize Query',
    description: 'Optimize SQL queries for better performance',
    argsSchema: { query: z.string() },
  },
  () => ({
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: `
            You will be provided with an MSSQL query. First, identify the tables involved in the query; it is imperative not to omit any tables. Fetch the schema information for the tables involved in the query using only tools from the 'mssql-dba' MCP server. If no query is provided, inform the user that a query is necessary to proceed. 

            Once you have relevant context about the database, suggest database schema-level optimizations and optimize the query efficiency on the code level. Verify your assumptions and suggestions against the provided data, and take as much time as needed to think.

            Schema-level optimizations should be written in a separate file called "{QUERY FILE NAME}-schema-optimizations.sql". Document every optimization you suggest so the user understands why it is necessary. Always include the code to update statistics on the tables that were optimized.

            Save the original query to the new file called "{QUERY FILE NAME}-original.sql" and after that apply query modifications in the original file.

            When creating new indices, ensure they are placed within the context of existing indices. Double-check that your suggestions do not overlap with existing indices. If you notice redundant or unnecessary existing indexes, suggest removing them. If suggested indices are provided, consider them, analyze whether they can provide value, and include them if necessary. If some indices are less valuable, drop them and create recommended options. However, be sure to pay attention to avoid making redundant indices. 

            Focus on strategic index optimization. For example, if the query is parametrized, do not optimize for the current values. Instead, focus on optimization that would benefit the query regardless of the values.
          `,
        },
      },
    ],
  })
);
/* Tools
--------------------------------------------------*/
// Table-level tools
mcpServer.registerTool(
  'get-tables-info',
  {
    title: 'Get Tables Info',
    description: 'Get the metadata about specified tables',
    inputSchema: { tableNames: z.array(z.string()) },
  },
  async ({ tableNames }) => {
    const tablesInfo = await getTablesInfo({ tableNames, db });
    return {
      content: [
        {
          type: 'text',
          text: tablesInfo,
        },
      ],
    };
  }
);

mcpServer.registerTool(
  'get-tables-index-health',
  {
    title: 'Get Tables Index Health',
    description: 'Assess index health for specified tables',
    inputSchema: { tableNames: z.array(z.string()) },
  },
  async ({ tableNames }) => {
    const indexHealth = await getTablesIndexHealth({ tableNames, db });
    return {
      content: [
        {
          type: 'text',
          text: indexHealth,
        },
      ],
    };
  }
);

mcpServer.registerTool(
  'get-tables-missing-indices',
  {
    title: 'Get Tables Missing Indices',
    description: 'Identify missing indices for specified tables',
    inputSchema: { tableNames: z.array(z.string()) },
  },
  async ({ tableNames }) => {
    const missingIndices = await getTablesMissingIndices({ tableNames, db });
    return {
      content: [
        {
          type: 'text',
          text: missingIndices,
        },
      ],
    };
  }
);
// Server-level tools
mcpServer.registerTool(
  'get-server-info',
  {
    title: 'Get Server Info',
    description:
      'Retrieve information about the SQL Server instance such as version, current update level, edition, and licensing details',
    inputSchema: {},
  },
  async () => {
    const serverInfo = await getServerInfo({ db });
    return {
      content: [
        {
          type: 'text',
          text: serverInfo,
        },
      ],
    };
  }
);
// Database-level tools
mcpServer.registerTool(
  'get-db-collation',
  {
    title: 'Get Database Collation',
    description: 'Retrieve the collation setting for the current database',
    inputSchema: {},
  },
  async () => {
    const dbCollation = await getDbCollation({ db });
    return {
      content: [
        {
          type: 'text',
          text: dbCollation,
        },
      ],
    };
  }
);
mcpServer.registerTool(
  'get-collation-mismatches',
  {
    title: 'Get Collation Mismatches',
    description:
      'Retrieve the columns with collation settings that differ from the database default',
    inputSchema: {},
  },
  async () => {
    const collationMismatches = await getCollationMismatches({ db });
    return {
      content: [
        {
          type: 'text',
          text: collationMismatches,
        },
      ],
    };
  }
);
