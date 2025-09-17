import { MSSQL } from '../../../MSSQL.js';

export async function getTablesIndexHealth({
  tableNames,
  db,
}: {
  tableNames: string[];
  db: MSSQL;
}): Promise<string> {
  if (tableNames.length === 0) {
    return 'No table names provided.';
  }
  const filterStatement = `tableName IN (${tableNames
    .map((name) => `'${name}'`)
    .join(', ')})`;

  const query = `-- =====================================================
-- Enhanced Index Health Monitor with Human-Readable Output
-- Evaluates current indexes and provides maintenance scripts
-- Excludes primary key indexes from analysis
-- =====================================================
WITH
    IndexUsageAnalysis AS (
        SELECT
            t.name AS tableName,
            i.name AS indexName,
            i.index_id,
            i.type_desc AS indexType,
            i.is_primary_key,
            i.is_unique,
            i.is_unique_constraint,
            -- Note: FILLFACTOR 0 means default (100% full)
            CASE
                WHEN i.fill_factor = 0 THEN 100
                ELSE i.fill_factor
            END AS actualFillFactor,
            s.user_seeks,
            s.user_scans,
            s.user_lookups,
            s.user_updates,
            s.user_seeks + s.user_scans + s.user_lookups AS totalReads,
            s.last_user_seek,
            s.last_user_scan,
            s.last_user_lookup,
            s.last_user_update,
            ps.avg_fragmentation_in_percent AS fragmentationPercent,
            ps.page_count AS pageCount,
            ps.avg_page_space_used_in_percent AS avgPageSpaceUsed,
            ps.record_count AS recordCount,
            CASE
                WHEN s.user_updates = 0 THEN 999999
                ELSE CAST(
                    s.user_seeks + s.user_scans + s.user_lookups AS FLOAT
                ) / s.user_updates
            END AS readWriteRatio,
            DATEDIFF(DAY, s.last_user_seek, GETDATE()) AS daysSinceLastSeek,
            DATEDIFF(DAY, s.last_user_scan, GETDATE()) AS daysSinceLastScan,
            DATEDIFF(DAY, s.last_user_update, GETDATE()) AS daysSinceLastUpdate,
            t.object_id,
            -- Calculate index size in bytes
            ps.page_count * 8 * 1024 AS indexSizeBytes
        FROM
            sys.indexes i
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id
            AND i.index_id = s.index_id
            AND s.database_id = DB_ID()
            OUTER APPLY sys.dm_db_index_physical_stats (DB_ID(), i.object_id, i.index_id, NULL, 'LIMITED') ps
        WHERE
            i.index_id > 0 -- Exclude heaps
            AND i.is_primary_key = 0 -- Exclude primary keys
            AND t.is_ms_shipped = 0
    ),
    KeyColumns AS (
        SELECT
            ic.object_id,
            ic.index_id,
            STRING_AGG(
                QUOTENAME(c.name) + CASE
                    WHEN ic.is_descending_key = 1 THEN ' DESC'
                    ELSE ''
                END,
                ', '
            ) WITHIN GROUP (
                ORDER BY
                    ic.key_ordinal
            ) AS keyColumns
        FROM
            sys.index_columns ic
            INNER JOIN sys.columns c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        WHERE
            ic.is_included_column = 0
        GROUP BY
            ic.object_id,
            ic.index_id
    ),
    IncludedColumns AS (
        SELECT
            ic.object_id,
            ic.index_id,
            STRING_AGG(QUOTENAME(c.name), ', ') WITHIN GROUP (
                ORDER BY
                    ic.index_column_id
            ) AS includedColumns
        FROM
            sys.index_columns ic
            INNER JOIN sys.columns c ON ic.object_id = c.object_id
            AND ic.column_id = c.column_id
        WHERE
            ic.is_included_column = 1
        GROUP BY
            ic.object_id,
            ic.index_id
    ),
    IndexHealthAssessment AS (
        SELECT
            iua.*,
            kc.keyColumns,
            ic.includedColumns,
            -- Determine if index is being used
            CASE
                WHEN iua.totalReads = 0
                AND iua.user_updates = 0 THEN 'NEVER_USED'
                WHEN iua.totalReads = 0
                AND iua.user_updates > 0 THEN 'WRITE_ONLY_OVERHEAD'
                WHEN iua.daysSinceLastSeek > 30
                AND iua.daysSinceLastScan > 30 THEN 'STALE'
                WHEN iua.readWriteRatio < 0.01 THEN 'WRITE_OVERHEAD'
                ELSE 'ACTIVE'
            END AS usageStatus,
            -- Optimal FILLFACTOR based on actual usage
            CASE
                WHEN iua.readWriteRatio >= 1000 THEN 100
                WHEN iua.readWriteRatio >= 100 THEN 95
                WHEN iua.readWriteRatio >= 10 THEN 90
                WHEN iua.readWriteRatio >= 1 THEN 85
                WHEN iua.readWriteRatio >= 0.1 THEN 80
                ELSE 70
            END AS optimalFillFactor,
            -- Health status
            CASE
                WHEN iua.totalReads = 0
                AND iua.is_unique_constraint = 0
                AND iua.is_unique = 0 THEN 'DROP_CANDIDATE'
                WHEN iua.fragmentationPercent > 30 THEN 'NEEDS_REBUILD'
                WHEN iua.fragmentationPercent > 10 THEN 'NEEDS_REORGANIZE'
                WHEN iua.avgPageSpaceUsed < 50
                AND iua.readWriteRatio > 10 THEN 'FILLFACTOR_TOO_LOW'
                WHEN iua.avgPageSpaceUsed > 95
                AND iua.readWriteRatio < 1 THEN 'FILLFACTOR_TOO_HIGH'
                ELSE 'HEALTHY'
            END AS healthStatus
        FROM
            IndexUsageAnalysis iua
            LEFT JOIN KeyColumns kc ON iua.object_id = kc.object_id
            AND iua.index_id = kc.index_id
            LEFT JOIN IncludedColumns ic ON iua.object_id = ic.object_id
            AND iua.index_id = ic.index_id
    )
SELECT
    -- Basic Information
    tableName AS [Table Name],
    indexName AS [Index Name],
    indexType AS [Index Type],
    -- Unique constraint information
    CASE
        WHEN is_unique_constraint = 1 THEN 'Yes (Constraint)'
        WHEN is_unique = 1 THEN 'Yes'
        ELSE 'No'
    END AS [Enforces Unique],
    keyColumns AS [Key Columns],
    ISNULL(includedColumns, 'None') AS [Included Columns],
    -- Usage Analysis with human-readable characterization
    CASE
        WHEN usageStatus = 'NEVER_USED' THEN 'Never Used'
        WHEN usageStatus = 'WRITE_ONLY_OVERHEAD' THEN 'Write-Only Overhead'
        WHEN usageStatus = 'STALE' THEN 'Stale (>30 days)'
        WHEN usageStatus = 'WRITE_OVERHEAD' THEN 'Write-Heavy'
        ELSE 'Active'
    END AS [Usage Status],
    -- Read/Write Pattern Analysis
    CASE
        WHEN readWriteRatio >= 1000 THEN 'Extremely Read-Heavy (' + FORMAT(CAST(readWriteRatio AS INT), 'N0') + ':1)'
        WHEN readWriteRatio >= 100 THEN 'Very Read-Heavy (' + FORMAT(CAST(readWriteRatio AS INT), 'N0') + ':1)'
        WHEN readWriteRatio >= 10 THEN 'Read-Heavy (' + FORMAT(CAST(readWriteRatio AS DECIMAL(10, 1)), 'N0') + ':1)'
        WHEN readWriteRatio >= 1 THEN 'Balanced (' + FORMAT(CAST(readWriteRatio AS DECIMAL(10, 1)), 'N0') + ':1)'
        WHEN readWriteRatio >= 0.1 THEN 'Write-Heavy (1:' + FORMAT(CAST(1 / readWriteRatio AS DECIMAL(10, 1)), 'N0') + ')'
        WHEN readWriteRatio > 0 THEN 'Very Write-Heavy (1:' + FORMAT(CAST(1 / readWriteRatio AS INT), 'N0') + ')'
        ELSE 'No Reads (Write-Only)'
    END AS [Read/Write Pattern],
    -- Formatted usage statistics
    --FORMAT(totalReads, 'N0') AS [Total Reads],
    --FORMAT(user_updates, 'N0') AS [Total Writes],
    -- Health Assessment
    CASE
        WHEN healthStatus = 'DROP_CANDIDATE' THEN 'Drop Candidate'
        WHEN healthStatus = 'NEEDS_REBUILD' THEN 'Needs Rebuild'
        WHEN healthStatus = 'NEEDS_REORGANIZE' THEN 'Needs Reorganize'
        WHEN healthStatus = 'FILLFACTOR_TOO_LOW' THEN 'Fill Factor Too Low'
        WHEN healthStatus = 'FILLFACTOR_TOO_HIGH' THEN 'Fill Factor Too High'
        ELSE 'Healthy'
    END AS [Health Status],
    -- Fragmentation with severity indicator
    CASE
        WHEN fragmentationPercent > 30 THEN 'Critical'
        WHEN fragmentationPercent > 10 THEN 'Warning'
        WHEN fragmentationPercent > 0 THEN 'Good'
        ELSE 'N/A'
    END AS [Fragmentation Status],
    CASE
        WHEN fragmentationPercent > 30 THEN FORMAT(fragmentationPercent, 'N1')
        WHEN fragmentationPercent > 10 THEN FORMAT(fragmentationPercent, 'N1')
        WHEN fragmentationPercent > 0 THEN FORMAT(fragmentationPercent, 'N1')
        ELSE 'N/A'
    END AS [Fragmentation %],
    -- Size information in human-readable format
    CASE
        WHEN indexSizeBytes >= 1073741824 THEN FORMAT(
            CAST(indexSizeBytes / 1073741824.0 AS DECIMAL(10, 2)),
            'N2'
        ) + ' GB'
        WHEN indexSizeBytes >= 1048576 THEN FORMAT(
            CAST(indexSizeBytes / 1048576.0 AS DECIMAL(10, 2)),
            'N2'
        ) + ' MB'
        WHEN indexSizeBytes >= 1024 THEN FORMAT(
            CAST(indexSizeBytes / 1024.0 AS DECIMAL(10, 2)),
            'N2'
        ) + ' KB'
        ELSE FORMAT(indexSizeBytes, 'N0') + ' Bytes'
    END AS [Index Size],
    FORMAT(pageCount, 'N0') AS [Page Count],
    FORMAT(recordCount, 'N0') AS [Record Count],
    -- Page space efficiency
    CASE
        WHEN avgPageSpaceUsed < 50 THEN 'Low (' + FORMAT(avgPageSpaceUsed, 'N1') + '%)'
        WHEN avgPageSpaceUsed < 70 THEN 'Fair (' + FORMAT(avgPageSpaceUsed, 'N1') + '%)'
        ELSE 'Good (' + FORMAT(avgPageSpaceUsed, 'N1') + '%)'
    END AS [Page Space Efficiency],
    -- Fill Factor Analysis
    CAST(actualFillFactor AS VARCHAR(3)) AS [Current Fill Factor %],
    CAST(optimalFillFactor AS VARCHAR(3)) AS [Recommended Fill Factor %],
    -- Last Activity with human-readable dates
    CASE
        WHEN last_user_seek IS NULL THEN 'Never'
        WHEN DATEDIFF(HOUR, last_user_seek, GETDATE()) < 1 THEN 'Within last hour'
        WHEN DATEDIFF(DAY, last_user_seek, GETDATE()) = 0 THEN 'Today'
        WHEN DATEDIFF(DAY, last_user_seek, GETDATE()) = 1 THEN 'Yesterday'
        WHEN DATEDIFF(DAY, last_user_seek, GETDATE()) < 7 THEN CAST(
            DATEDIFF(DAY, last_user_seek, GETDATE()) AS VARCHAR(10)
        ) + ' days ago'
        WHEN DATEDIFF(WEEK, last_user_seek, GETDATE()) < 4 THEN CAST(
            DATEDIFF(WEEK, last_user_seek, GETDATE()) AS VARCHAR(10)
        ) + ' weeks ago'
        WHEN DATEDIFF(MONTH, last_user_seek, GETDATE()) < 12 THEN CAST(
            DATEDIFF(MONTH, last_user_seek, GETDATE()) AS VARCHAR(10)
        ) + ' months ago'
        ELSE CAST(
            DATEDIFF(YEAR, last_user_seek, GETDATE()) AS VARCHAR(10)
        ) + ' years ago'
    END AS [Last Seek],
    CASE
        WHEN last_user_scan IS NULL THEN 'Never'
        WHEN DATEDIFF(HOUR, last_user_scan, GETDATE()) < 1 THEN 'Within last hour'
        WHEN DATEDIFF(DAY, last_user_scan, GETDATE()) = 0 THEN 'Today'
        WHEN DATEDIFF(DAY, last_user_scan, GETDATE()) = 1 THEN 'Yesterday'
        WHEN DATEDIFF(DAY, last_user_scan, GETDATE()) < 7 THEN CAST(
            DATEDIFF(DAY, last_user_scan, GETDATE()) AS VARCHAR(10)
        ) + ' days ago'
        WHEN DATEDIFF(WEEK, last_user_scan, GETDATE()) < 4 THEN CAST(
            DATEDIFF(WEEK, last_user_scan, GETDATE()) AS VARCHAR(10)
        ) + ' weeks ago'
        WHEN DATEDIFF(MONTH, last_user_scan, GETDATE()) < 12 THEN CAST(
            DATEDIFF(MONTH, last_user_scan, GETDATE()) AS VARCHAR(10)
        ) + ' months ago'
        ELSE CAST(
            DATEDIFF(YEAR, last_user_scan, GETDATE()) AS VARCHAR(10)
        ) + ' years ago'
    END AS [Last Scan],
    CASE
        WHEN last_user_update IS NULL THEN 'Never'
        WHEN DATEDIFF(HOUR, last_user_update, GETDATE()) < 1 THEN 'Within last hour'
        WHEN DATEDIFF(DAY, last_user_update, GETDATE()) = 0 THEN 'Today'
        WHEN DATEDIFF(DAY, last_user_update, GETDATE()) = 1 THEN 'Yesterday'
        WHEN DATEDIFF(DAY, last_user_update, GETDATE()) < 7 THEN CAST(
            DATEDIFF(DAY, last_user_update, GETDATE()) AS VARCHAR(10)
        ) + ' days ago'
        WHEN DATEDIFF(WEEK, last_user_update, GETDATE()) < 4 THEN CAST(
            DATEDIFF(WEEK, last_user_update, GETDATE()) AS VARCHAR(10)
        ) + ' weeks ago'
        WHEN DATEDIFF(MONTH, last_user_update, GETDATE()) < 12 THEN CAST(
            DATEDIFF(MONTH, last_user_update, GETDATE()) AS VARCHAR(10)
        ) + ' months ago'
        ELSE CAST(
            DATEDIFF(YEAR, last_user_update, GETDATE()) AS VARCHAR(10)
        ) + ' years ago'
    END AS [Last Update],
    -- Maintenance recommendation with priority
    CASE
        WHEN healthStatus = 'DROP_CANDIDATE'
        AND is_unique = 0 THEN 'HIGH - Drop Index'
        WHEN healthStatus = 'DROP_CANDIDATE'
        AND is_unique = 1 THEN 'MEDIUM - Review Unique Index'
        WHEN healthStatus = 'NEEDS_REBUILD' THEN 'HIGH - Rebuild'
        WHEN healthStatus = 'NEEDS_REORGANIZE' THEN 'MEDIUM - Reorganize'
        WHEN healthStatus IN ('FILLFACTOR_TOO_LOW', 'FILLFACTOR_TOO_HIGH') THEN 'MEDIUM - Adjust Fill Factor'
        ELSE 'LOW - No Action Needed'
    END AS [Maintenance Priority],
    -- Detailed reason for recommendation
    CASE
        WHEN totalReads = 0
        AND is_unique = 0 THEN 'Index has never been used and is consuming ' + CASE
            WHEN indexSizeBytes >= 1073741824 THEN FORMAT(
                CAST(indexSizeBytes / 1073741824.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' GB'
            WHEN indexSizeBytes >= 1048576 THEN FORMAT(
                CAST(indexSizeBytes / 1048576.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' MB'
            ELSE FORMAT(
                CAST(indexSizeBytes / 1024.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' KB'
        END + ' of storage. Safe to drop.'
        WHEN totalReads = 0
        AND is_unique = 1 THEN 'Unique index has never been used for queries. Review if uniqueness constraint is still needed. Size: ' + CASE
            WHEN indexSizeBytes >= 1073741824 THEN FORMAT(
                CAST(indexSizeBytes / 1073741824.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' GB'
            WHEN indexSizeBytes >= 1048576 THEN FORMAT(
                CAST(indexSizeBytes / 1048576.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' MB'
            ELSE FORMAT(
                CAST(indexSizeBytes / 1024.0 AS DECIMAL(10, 2)),
                'N2'
            ) + ' KB'
        END
        WHEN fragmentationPercent > 30 THEN 'High fragmentation (' + FORMAT(fragmentationPercent, 'N1') + '%) is degrading performance. Rebuild recommended.'
        WHEN fragmentationPercent > 10 THEN 'Moderate fragmentation (' + FORMAT(fragmentationPercent, 'N1') + '%) detected. Reorganize to improve performance.'
        WHEN actualFillFactor != optimalFillFactor
        AND ABS(actualFillFactor - optimalFillFactor) > 10 THEN 'Fill factor mismatch: Current=' + CAST(actualFillFactor AS VARCHAR(3)) + '%, Optimal=' + CAST(optimalFillFactor AS VARCHAR(3)) + '% based on ' + CASE
            WHEN readWriteRatio >= 10 THEN 'read-heavy'
            WHEN readWriteRatio >= 1 THEN 'balanced'
            ELSE 'write-heavy'
        END + ' workload pattern.'
        ELSE 'Index is performing well with good fragmentation levels and appropriate fill factor.'
    END AS [Detailed Analysis],
    -- Generate maintenance script
    CASE
        WHEN totalReads = 0
        AND is_unique = 0 THEN 'DROP INDEX [' + indexName + '] ON [dbo].[' + tableName + '];'
        WHEN totalReads = 0
        AND is_unique = 1 THEN '-- Review before dropping unique index: DROP INDEX [' + indexName + '] ON [dbo].[' + tableName + '];'
        WHEN fragmentationPercent > 30
        OR (
            actualFillFactor != optimalFillFactor
            AND ABS(actualFillFactor - optimalFillFactor) > 10
        ) THEN 'ALTER INDEX [' + indexName + '] ON [dbo].[' + tableName + '] ' + 'REBUILD WITH (' + 'FILLFACTOR = ' + CAST(optimalFillFactor AS VARCHAR(3)) + ', ' + 'PAD_INDEX = ' + CASE
            WHEN optimalFillFactor < 90 THEN 'ON'
            ELSE 'OFF'
        END + ', ' + 'SORT_IN_TEMPDB = ON, ' + 'ONLINE = ' + CASE
            WHEN pageCount > 1000 THEN 'ON'
            ELSE 'OFF'
        END + ', ' + 'MAXDOP = 1, ' + 'DATA_COMPRESSION = ' + CASE
            WHEN pageCount > 1000
            AND readWriteRatio >= 10 THEN 'PAGE'
            WHEN pageCount > 1000
            AND readWriteRatio >= 1 THEN 'ROW'
            ELSE 'NONE'
        END + ');'
        WHEN fragmentationPercent > 10 THEN 'ALTER INDEX [' + indexName + '] ON [dbo].[' + tableName + '] REORGANIZE;'
        ELSE '-- No maintenance required'
    END AS [Maintenance Script]
FROM
    IndexHealthAssessment
WHERE
    ${filterStatement};`;

  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No indices found with the specified tables.';
  }
  return JSON.stringify(result, null, 2);
}
