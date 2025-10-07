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

const mssqlConfig: MSSQLConfig = {
  user: process.env.DB_USER || '',
  database: process.env.DB_NAME || '',
  password: process.env.DB_PASSWORD || '',
  server: process.env.DB_HOST || '',
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
    argsSchema: {},
  },
  () => ({
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: `You will be provided with an MSSQL query. First, identify the tables involved in the query; it is imperative not to omit any tables. Fetch the schema information for the tables involved in the query using only tools from the 'mssql-dba' MCP server. If no query is provided, inform the user that a query is necessary to proceed. 

Once you have relevant context about the database, suggest database schema-level optimizations and optimize the query efficiency on the code level. Verify your assumptions and suggestions against the provided data, and take as much time as needed to think.

Focus on identifying inefficient indices and suggest removing them. Also, provide options for index consolidation, where possible. Analyze missing indices and include them if necessary. After that, analyze the query and consider whether you can think of additional indices that could enhance the performance. However, be sure to pay attention to avoid making redundant indices. When creating new indices, ensure they are placed within the context of existing ones.

Focus on strategic index optimization. For example, if the query is parametrized, do not optimize for the current values. Instead, focus on optimization that would benefit the query regardless of the values.

Do not limit yourself only to indexing optimizations. If you can think of other techniques that are more suitable, based on the size of the tables or other factors, please suggest them.

For each recommendation, explain why it's beneficial and document it in the file.
Important: do not make up optimizations if they are unnecessary. If the query is already efficient and the tables have decent indexing, inform the user about this. Do not create the files if no optimizations are necessary.

Schema-level optimizations should be written in a separate file called “{QUERY FILE NAME}-schema-optimizations.sql”. Document every optimization you suggest so the user understands why it is necessary. Always include the code to update statistics on the tables that were optimized.

Important: do not make up optimizations if they are unnecessary. If the query is already efficient and the tables have decent indexing, inform the user about this. Do not create the files if no optimizations are necessary.

Query code changes should be written to the file called “{QUERY FILE NAME}-optimized.sql”. 

Include the file with the rollback steps of all the suggested optimizations in the file "rollback-script.sql".
`,
        },
      },
    ],
  })
);
mcpServer.registerPrompt(
  'optimize-indices',
  {
    title: 'Optimize Indices',
    description: 'Optimize indices on specified tables.',
    argsSchema: { tableNames: z.string() },
  },
  ({ tableNames }) => ({
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: `Fetch the information about existing and missing indices for these tables: ${tableNames}, using only tools from the 'mssql-dba' MCP server. If you receive an error from any of the tools, stop right away and inform the user about the error. If no tables are provided, inform the user that at least one table is necessary to proceed. 

Once you have relevant context about the tables, suggest indexing optimizations. Verify your assumptions and suggestions against the provided data, and take as much time as needed to think.

Focus on identifying inefficient indices and suggest removing them.  Also, provide options for index consolidation, where possible. Analyze missing indices and include them if necessary. However, be sure to pay attention to avoid making redundant indices.

Important: do not make up optimizations if they are unnecessary. If the tables have decent indexing, inform the user about this and suggest scheduling maintenance that will keep the indices healthy.

Indexing optimizations should be written in a file called "index-optimizations.sql". Document every optimization you suggest so the user understands why it is necessary. Always include the code to update statistics on the tables that were optimized.

Include the file with the rollback steps of all the suggested optimizations in the file "rollback-script.sql" 

Include the file "index-maintenance.sql," which contains scheduled stored procedures that maintain the health of indices. For example (but not limited to), include scheduled statistics updates and scheduled fragmentation treatment.
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
    try {
      const tablesInfo = await getTablesInfo({ tableNames, db });
      return {
        content: [
          {
            type: 'text',
            text: tablesInfo,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving tables info: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
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
    try {
      const indexHealth = await getTablesIndexHealth({ tableNames, db });
      return {
        content: [
          {
            type: 'text',
            text: indexHealth,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving index health: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
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
    try {
      const missingIndices = await getTablesMissingIndices({ tableNames, db });
      return {
        content: [
          {
            type: 'text',
            text: missingIndices,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving missing indices: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
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
    try {
      const serverInfo = await getServerInfo({ db });
      return {
        content: [
          {
            type: 'text',
            text: serverInfo,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving server info: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
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
    try {
      const dbCollation = await getDbCollation({ db });
      return {
        content: [
          {
            type: 'text',
            text: dbCollation,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving database collation: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
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
    try {
      const collationMismatches = await getCollationMismatches({ db });
      return {
        content: [
          {
            type: 'text',
            text: collationMismatches,
          },
        ],
      };
    } catch (error) {
      return {
        content: [
          {
            type: 'text',
            text: `Error retrieving collation mismatches: ${
              error instanceof Error ? error.message : String(error)
            }`,
          },
        ],
      };
    }
  }
);
