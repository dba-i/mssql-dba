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
    this.ensureConnected();
    const request = this.pool!.request();
    if (parameters) {
      for (const [key, value] of Object.entries(parameters)) {
        request.input(key, value);
      }
    }
    const result = await request.query(query);
    return result.recordset;
  }

  /**
   * Fetches metadata about the database such as size and free space in MB.
   * @returns An object containing dbSizeMB and dbFreeSpaceMB.
   */
  async getDatabaseMetadata(): Promise<{
    dbSizeMB: number;
    dbFreeSpaceMB: number;
  }> {
    this.ensureConnected();
    // Query to get database size in MB
    const dbSizeQuery = `
      SELECT
        SUM(size) * 8.0 / 1024 AS dbSizeMB
      FROM sys.database_files;
    `;
    // Query to get free space in MB
    const dbFreeSpaceQuery = `
      SELECT SUM(size - CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)) * 8.0 / 1024 AS dbFreeSpaceMB
      FROM sys.database_files;
    `;
    const dbSizeResult = await this.executeQuery<{ dbSizeMB: number }>({
      query: dbSizeQuery,
    });
    const dbFreeSpaceResult = await this.executeQuery<{
      dbFreeSpaceMB: number;
    }>({ query: dbFreeSpaceQuery });
    const dbSizeMB = dbSizeResult[0]?.dbSizeMB ?? 0;
    const dbFreeSpaceMB = dbFreeSpaceResult[0]?.dbFreeSpaceMB ?? 0;
    return { dbSizeMB, dbFreeSpaceMB };
  }
  // Utilities for permissions
  async checkPermissions(): Promise<Record<string, string[]>> {
    this.ensureConnected();

    const query = fs.readFileSync(
      'src/sql/mssql/permissions/checkPermissions.sql',
      'utf8'
    );
    const permissions = await this.executeQuery<{
      level: string;
      permission_name: string;
    }>({ query });

    return permissions.reduce((acc, { level, permission_name }) => {
      if (!acc[level]) {
        acc[level] = [];
      }
      if (!acc[level].includes(permission_name)) {
        acc[level].push(permission_name);
      }
      return acc;
    }, {} as Record<string, string[]>);
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
