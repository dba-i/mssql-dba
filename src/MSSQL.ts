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
 * Singleton pattern ensures only one instance exists.
 */
export class MSSQL {
  private static instance: MSSQL | null = null;
  private pool: sql.ConnectionPool | null = null;
  private config: MSSQLConfig;

  private constructor(config: MSSQLConfig) {
    this.config = config;
  }

  /**
   * Gets the singleton instance of MSSQL.
   * @param config - Configuration for the database connection (required on first call)
   * @returns The singleton instance
   */
  public static getInstance(config: MSSQLConfig): MSSQL {
    if (!MSSQL.instance) {
      MSSQL.instance = new MSSQL(config);
    }
    return MSSQL.instance;
  }

  /**
   * Resets the singleton instance (useful for testing or reconfiguration).
   */
  public static resetInstance(): void {
    if (MSSQL.instance?.pool) {
      MSSQL.instance.pool.close();
    }
    MSSQL.instance = null;
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
