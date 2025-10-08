import { MSSQL } from '../../../MSSQL.js';
export async function getTablesMissingIndexes({
  tableNames,
  db,
}: {
  tableNames: string[];
  db: MSSQL;
}): Promise<string> {
  if (tableNames.length === 0) {
    return 'No table names provided.';
  }
  const filterStatement = `AND tableName IN (${tableNames
    .map((name) => `'${name}'`)
    .join(', ')})`;

  const query = `-- =====================================================
-- Missing Index Recommender with Optimal Parameters
-- Generates CREATE INDEX statements based on workload
-- FILLFACTOR explanation: 0 or 100 = pages 100% full
--                         lower values leave free space
-- =====================================================
WITH
    TableWorkload AS (
        SELECT
            t.object_id,
            t.name AS tableName,
            p.rows AS [rowCount],
            ISNULL(
                SUM(us.user_seeks + us.user_scans + us.user_lookups),
                0
            ) AS totalReads,
            ISNULL(SUM(us.user_updates), 0) AS totalWrites,
            CASE
                WHEN ISNULL(SUM(us.user_updates), 0) = 0 THEN 999999
                ELSE CAST(
                    ISNULL(
                        SUM(us.user_seeks + us.user_scans + us.user_lookups),
                        0
                    ) AS FLOAT
                ) / NULLIF(SUM(us.user_updates), 0)
            END AS readWriteRatio
        FROM
            sys.tables t
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            AND p.index_id IN (0, 1)
            LEFT JOIN sys.dm_db_index_usage_stats us ON t.object_id = us.object_id
            AND us.database_id = DB_ID()
        WHERE
            t.is_ms_shipped = 0
        GROUP BY
            t.object_id,
            t.name,
            p.rows
    ),
    MissingIndexDetails AS (
        SELECT
            mid.object_id,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            mid.statement,
            migs.user_seeks,
            migs.user_scans,
            migs.avg_total_user_cost,
            migs.avg_user_impact,
            migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure,
            migs.last_user_seek,
            ROW_NUMBER() OVER (
                PARTITION BY
                    mid.object_id
                ORDER BY
                    migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) DESC
            ) AS index_rank
        FROM
            sys.dm_db_missing_index_groups mig
            INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
            INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE
            mid.database_id = DB_ID()
    ),
    IndexRecommendations AS (
        SELECT
            tw.tableName,
            tw.[rowCount],
            tw.readWriteRatio,
            mid.improvement_measure AS improvementScore,
            mid.avg_user_impact AS expectedImprovementPercent,
            mid.user_seeks + mid.user_scans AS queriesThatWouldBenefit,
            mid.last_user_seek AS lastTimeNeeded,
            mid.equality_columns,
            mid.inequality_columns,
            mid.included_columns,
            -- Workload-based parameters
            CASE
                WHEN tw.readWriteRatio >= 1000 THEN 'Read-Only'
                WHEN tw.readWriteRatio >= 100 THEN 'Read-Heavy'
                WHEN tw.readWriteRatio >= 10 THEN 'Read-Moderate'
                WHEN tw.readWriteRatio >= 1 THEN 'Balanced'
                WHEN tw.readWriteRatio >= 0.1 THEN 'Write-Moderate'
                ELSE 'Write-Heavy'
            END AS workloadType,
            -- Optimal FILLFACTOR based on workload
            CASE
                WHEN tw.[rowCount] < 1000 THEN 100 -- Tiny tables: no free space needed
                WHEN tw.readWriteRatio >= 1000 THEN 100 -- Read-only: maximize density
                WHEN tw.readWriteRatio >= 100 THEN 95 -- Read-heavy: minimal free space
                WHEN tw.readWriteRatio >= 10 THEN 90 -- Read-moderate: some free space
                WHEN tw.readWriteRatio >= 1 THEN 85 -- Balanced: moderate free space
                WHEN tw.readWriteRatio >= 0.1 THEN 80 -- Write-moderate: more free space
                ELSE 70 -- Write-heavy: maximum free space for inserts
            END AS recommendedFillFactor,
            -- PAD_INDEX recommendation
            CASE
                WHEN tw.[rowCount] < 10000 THEN 'OFF' -- Small tables don't benefit
                WHEN tw.readWriteRatio >= 100 THEN 'OFF' -- Read-heavy doesn't need padding
                ELSE 'ON' -- Write workloads benefit from padding intermediate levels
            END AS recommendedPadIndex,
            -- Priority assessment
            CASE
                WHEN mid.improvement_measure > 10000 THEN 'CRITICAL'
                WHEN mid.improvement_measure > 1000 THEN 'HIGH'
                WHEN mid.improvement_measure > 100 THEN 'MEDIUM'
                WHEN mid.improvement_measure > 10 THEN 'LOW'
                ELSE 'MINIMAL'
            END AS priority,
            -- Should we create this index? (Keep original logic)
            CASE
                WHEN tw.[rowCount] < 1000 THEN 0
                WHEN mid.index_rank > 3 THEN 0
                WHEN mid.improvement_measure < 10 THEN 0
                WHEN tw.totalReads < 100 THEN 0
                ELSE 1
            END AS shouldCreateIndex,
            -- Reason for decision (Without urgency mentions)
            CASE
                WHEN tw.[rowCount] < 1000 THEN 'Table < 1000 rows'
                WHEN mid.index_rank > 3 THEN 'Too many indexes'
                WHEN mid.improvement_measure < 10 THEN 'Minimal benefit'
                WHEN tw.totalReads < 100 THEN 'Low read activity'
                ELSE 'Recommended'
            END AS decisionReason,
            mid.index_rank,
            tw.totalReads,
            tw.totalWrites
        FROM
            MissingIndexDetails mid
            INNER JOIN TableWorkload tw ON mid.object_id = tw.object_id
    )
SELECT
    -- Basic Information
    tableName AS [Table],
    CASE priority
        WHEN 'CRITICAL' THEN 'CRITICAL'
        WHEN 'HIGH' THEN 'HIGH'
        WHEN 'MEDIUM' THEN 'MEDIUM'
        WHEN 'LOW' THEN 'LOW'
        ELSE 'MINIMAL'
    END AS [Urgency],
    CASE shouldCreateIndex
        WHEN 1 THEN 'Yes'
        ELSE 'No'
    END AS [Should Create],
    decisionReason AS [Reason],
    -- Impact Analysis
    expectedImprovementPercent AS [Improvement %],
    queriesThatWouldBenefit AS [Query Uses],
    -- Time Analysis
    CASE
        WHEN lastTimeNeeded IS NULL THEN 'Never'
        WHEN DATEDIFF(HOUR, lastTimeNeeded, GETDATE()) < 24 THEN 'Today'
        WHEN DATEDIFF(DAY, lastTimeNeeded, GETDATE()) < 7 THEN CAST(
            DATEDIFF(DAY, lastTimeNeeded, GETDATE()) AS VARCHAR
        ) + 'd'
        WHEN DATEDIFF(DAY, lastTimeNeeded, GETDATE()) < 30 THEN CAST(
            DATEDIFF(WEEK, lastTimeNeeded, GETDATE()) AS VARCHAR
        ) + 'w'
        ELSE CAST(
            DATEDIFF(MONTH, lastTimeNeeded, GETDATE()) AS VARCHAR
        ) + 'm'
    END AS [Last Needed],
    -- Index Configuration
    recommendedFillFactor AS [Fill Factor],
    recommendedPadIndex AS [Pad Index],
    CASE
        WHEN [rowCount] > 100000
        AND readWriteRatio >= 10 THEN 'PAGE'
        WHEN [rowCount] > 100000
        AND readWriteRatio >= 1 THEN 'ROW'
        ELSE 'NONE'
    END AS [Compression],
    -- Index columns
    ISNULL(equality_columns, '') AS [Equality Columns],
    ISNULL(inequality_columns, '') AS [Inequality Columns],
    ISNULL(included_columns, '') AS [Included Columns]
FROM
    IndexRecommendations
WHERE
    index_rank <= 5 -- Limit to top 5 missing indexes per table 
    ${filterStatement};`;
  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No missing index recommendations found for the specified tables. State to the user that everything is fine.';
  }
  return JSON.stringify(result, null, 2);
}
