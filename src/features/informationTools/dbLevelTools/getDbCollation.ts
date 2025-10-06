import { MSSQL } from '../../../MSSQL.js';

export async function getDbCollation({
  db,
}: {
  db: MSSQL;
}): Promise<string> {

  const query = `SELECT DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS [Database Collation]`;

  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No collation information found.';
  }
  return JSON.stringify(result, null, 2);
}
