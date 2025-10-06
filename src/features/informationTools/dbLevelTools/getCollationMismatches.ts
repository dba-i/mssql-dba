import { MSSQL } from '../../../MSSQL.js';

export async function getCollationMismatches({
  db,
}: {
  db: MSSQL;
}): Promise<string> {

  const query = `
  SELECT 
    t.name AS [Table Name],
    c.name AS [Column Name],
    c.collation_name AS [Column Collation],
    ty.name AS [Data Type],
    c.max_length AS [Max Length]
FROM 
    sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
WHERE 
    c.collation_name IS NOT NULL
    AND c.collation_name <> CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS VARCHAR(100))
    AND s.name = 'dbo'
ORDER BY 
    s.name, t.name, c.name;`;

  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No collation mismatches found.';
  }
  return JSON.stringify(result, null, 2);
}
