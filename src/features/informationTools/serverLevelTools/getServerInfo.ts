import { MSSQL } from '../../../MSSQL.js';

export async function getServerInfo({
  db,
}: {
  db: MSSQL;
}): Promise<string> {

  const query = `
  SELECT 
    -- SQL Server Version Information
    SERVERPROPERTY('ProductVersion') AS [Product Version],
    SERVERPROPERTY('ProductLevel') AS [Product Level],
    SERVERPROPERTY('ProductUpdateLevel') AS [Product Update Level],
    SERVERPROPERTY('ProductUpdateReference') AS [Product Update Reference],
    -- Edition Information
    CASE SERVERPROPERTY('EngineEdition')
        WHEN 1 THEN 'Personal or Desktop Engine'
        WHEN 2 THEN 'Standard'
        WHEN 3 THEN 'Enterprise'
        WHEN 4 THEN 'Express'
        WHEN 5 THEN 'SQL Database'
        WHEN 6 THEN 'SQL Data Warehouse'
        WHEN 8 THEN 'Managed Instance'
        WHEN 9 THEN 'Azure SQL Edge'
        WHEN 11 THEN 'Azure Synapse serverless SQL pool'
        ELSE 'Unknown'
    END AS [Engine Edition Description],
    CASE 
        WHEN SERVERPROPERTY('EngineEdition') = 3 THEN 'Yes'
        ELSE 'No'
    END AS [Is Enterprise Edition],
    
    -- Server Information
    SERVERPROPERTY('ServerName') AS [Server Name],
    SERVERPROPERTY('Collation') AS [Server Collation],
    SERVERPROPERTY('IsClustered') AS [Is Clustered],
    SERVERPROPERTY('IsHadrEnabled') AS [Is HADR Enabled],
    SERVERPROPERTY('HadrManagerStatus') AS [HADR Manager Status],
    
    -- Licensing Information
    SERVERPROPERTY('LicenseType') AS [License Type],
    SERVERPROPERTY('NumLicenses') AS [Number of Licenses]`;

  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No server information found.';
  }
  return JSON.stringify(result, null, 2);
}
