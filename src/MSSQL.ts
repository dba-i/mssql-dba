import sql from 'mssql';
import fs from 'fs';

export interface MSSQLConfig {
  user: string;
  password: string;
  server: string;
  database: string;
  options?: sql.IOptions; // Leverage the existing type
  pool?: {
    max?: number;
    min?: number;
    idleTimeoutMillis?: number;
  };
}

/**
 * Represents a connection to an MSSQL database.
 */
export class MSSQL {
  private pool: sql.ConnectionPool | null = null;
  private config: MSSQLConfig;

  constructor(config: MSSQLConfig) {
    this.config = config;
  }
  async executeQuery<T>({
    query,
    parameters,
  }: {
    query: string;
    parameters?: Record<string, any>;
  }): Promise<T[]> {
    await this.connect();
    const request = this.pool!.request();
    if (parameters) {
      for (const [key, value] of Object.entries(parameters)) {
        request.input(key, value);
      }
    }
    const result = await request.query(query);
    return result.recordset;
  }
  // Utilities for connection management
  async connect(): Promise<void> {
    if (this.isConnected()) {
      return;
    }
    this.pool = await sql.connect(this.config);
  }

  async disconnect(): Promise<void> {
    await this.pool!.close();
    this.pool = null;
  }

  isConnected(): boolean {
    return this.pool !== null;
  }

  ensureConnected(): void {
    if (!this.isConnected()) {
      throw new Error('Database connection is not established.');
    }
  }
}
