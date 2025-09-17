#!/usr/bin/env node
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { mcpServer } from './stdioServer.js';

async function main() {
  const transport = new StdioServerTransport();
  await mcpServer.connect(transport);
  console.error('MSSQL DBA MCP Server running on stdio');
}

main().catch((error) => {
  console.error('Fatal error in main():', error);
  process.exit(1);
});
