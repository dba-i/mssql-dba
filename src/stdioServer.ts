import { getTablesMissingIndices } from './features/informationTools/tableLevelTools/getTablesMissingIndices.js';
import { getTablesIndexHealth } from './features/informationTools/tableLevelTools/getTablesIndexHealth.js';
import { getTablesInfo } from './features/informationTools/tableLevelTools/getTablesInfo.js';
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
const db = new MSSQL(mssqlConfig);
await db.connect();

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
          text: `You are provided with metadata about specified tables, in the form of JSON. Tables info: ${tablesInfo}`,
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
          text: `You are provided with index health information on specified tables, in the form of JSON. Index health: ${indexHealth}`,
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
          text: `You are provided with missing indices on the specified tables, in the form of JSON. Missing indices: ${missingIndices}`,
        },
      ],
    };
  }
);

// Prompts
mcpServer.registerPrompt(
  'optimize-query',
  {
    title: 'Optimize Query',
    description: 'Optimize SQL queries for better performance',
    argsSchema: { query: z.string() },
  },
  ({ query }) => ({
    messages: [
      {
        role: 'user',
        content: {
          type: 'text',
          text: `You will be provided with an MSSQL query delimited by triple quotes. Fetch the schema information for the tables involved in the query and suggest schema-level optimizations. Also, highlight the inefficiencies in the query, but prioritize schema-level optimizations first. Provide the user with step-by-step guidance on how to implement your suggestions. Query:\n\n"""\n${query}\n"""`,
        },
      },
    ],
  })
);
