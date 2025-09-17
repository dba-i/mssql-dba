import { MSSQL } from '../../../MSSQL.js';

export async function getActiveTablesInfo({
  db,
}: {
  db: MSSQL;
}): Promise<string> {
  const query = `-- =====================================================
-- Enhanced Table Workload Analyzer with Primary Key and Index Information
-- Human-Friendly Output Version - Extended with Index Metrics
-- =====================================================
WITH
    PrimaryKeyInfo AS (
        -- Get primary key information for each table
        SELECT
            t.object_id,
            t.name AS tableName,
            i.name AS primaryKeyName,
            i.type_desc AS indexType,
            i.is_unique,
            i.is_primary_key,
            i.is_unique_constraint,
            i.fill_factor,
            i.is_padded,
            i.is_disabled,
            i.allow_row_locks,
            i.allow_page_locks,
            i.has_filter,
            STUFF(
                (
                    SELECT
                        ',' + c.name + ' (' + CASE
                            WHEN ic.is_descending_key = 1 THEN 'DESC'
                            ELSE 'ASC'
                        END + ')'
                    FROM
                        sys.index_columns ic
                        INNER JOIN sys.columns c ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                    WHERE
                        ic.object_id = i.object_id
                        AND ic.index_id = i.index_id
                    ORDER BY
                        ic.key_ordinal
                    FOR XML
                        PATH ('')
                ),
                1,
                1,
                ''
            ) AS keyColumns,
            STUFF(
                (
                    SELECT
                        ',' + ty.name + CASE
                            WHEN ty.name IN (
                                'varchar',
                                'char',
                                'nvarchar',
                                'nchar',
                                'binary',
                                'varbinary'
                            ) THEN '(' + CASE
                                WHEN c.max_length = -1 THEN 'MAX'
                                WHEN ty.name IN ('nvarchar', 'nchar') THEN CAST(c.max_length / 2 AS VARCHAR(10))
                                ELSE CAST(c.max_length AS VARCHAR(10))
                            END + ')'
                            WHEN ty.name IN ('decimal', 'numeric') THEN '(' + CAST(c.precision AS VARCHAR(10)) + ',' + CAST(c.scale AS VARCHAR(10)) + ')'
                            ELSE ''
                        END + CASE
                            WHEN c.is_identity = 1 THEN ' IDENTITY'
                            ELSE ''
                        END
                    FROM
                        sys.index_columns ic
                        INNER JOIN sys.columns c ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                        INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                    WHERE
                        ic.object_id = i.object_id
                        AND ic.index_id = i.index_id
                    ORDER BY
                        ic.key_ordinal
                    FOR XML
                        PATH ('')
                ),
                1,
                1,
                ''
            ) AS keyDataTypes,
            -- Count of key columns
            (
                SELECT
                    COUNT(*)
                FROM
                    sys.index_columns ic
                WHERE
                    ic.object_id = i.object_id
                    AND ic.index_id = i.index_id
                    AND ic.is_included_column = 0
            ) AS keyColumnCount,
            -- Check if any key column is identity
            CASE
                WHEN EXISTS (
                    SELECT
                        1
                    FROM
                        sys.index_columns ic
                        INNER JOIN sys.columns c ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                    WHERE
                        ic.object_id = i.object_id
                        AND ic.index_id = i.index_id
                        AND c.is_identity = 1
                ) THEN 1
                ELSE 0
            END AS hasIdentityKey,
            -- Check if PK is on computed columns
            CASE
                WHEN EXISTS (
                    SELECT
                        1
                    FROM
                        sys.index_columns ic
                        INNER JOIN sys.columns c ON ic.object_id = c.object_id
                        AND ic.column_id = c.column_id
                    WHERE
                        ic.object_id = i.object_id
                        AND ic.index_id = i.index_id
                        AND c.is_computed = 1
                ) THEN 1
                ELSE 0
            END AS hasComputedKey,
            -- Get the first key column type for categorization
            (
                SELECT
                    TOP 1 ty.name
                FROM
                    sys.index_columns ic
                    INNER JOIN sys.columns c ON ic.object_id = c.object_id
                    AND ic.column_id = c.column_id
                    INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                WHERE
                    ic.object_id = i.object_id
                    AND ic.index_id = i.index_id
                ORDER BY
                    ic.key_ordinal
            ) AS firstKeyDataType,
            -- Index size
            ps.used_page_count * 8.0 / 1024 AS indexSizeMB,
            ps.row_count AS indexRowCount
        FROM
            sys.tables t
            LEFT JOIN sys.indexes i ON t.object_id = i.object_id
            AND i.is_primary_key = 1
            LEFT JOIN sys.dm_db_partition_stats ps ON i.object_id = ps.object_id
            AND i.index_id = ps.index_id
        WHERE
            t.is_ms_shipped = 0
    ),
    IndexMetrics AS (
        -- Get index statistics for each table
        SELECT
            t.object_id,
            COUNT(DISTINCT i.index_id) - 1 AS totalIndexCount, -- Exclude heap/clustered
            COUNT(
                DISTINCT CASE
                    WHEN i.is_disabled = 0
                    AND i.index_id > 0 THEN i.index_id
                END
            ) AS activeIndexCount,
            COUNT(
                DISTINCT CASE
                    WHEN i.is_disabled = 1 THEN i.index_id
                END
            ) AS disabledIndexCount,
            COUNT(
                DISTINCT CASE
                    WHEN i.is_unique = 1
                    AND i.is_primary_key = 0 THEN i.index_id
                END
            ) AS uniqueIndexCount,
            COUNT(
                DISTINCT CASE
                    WHEN i.has_filter = 1 THEN i.index_id
                END
            ) AS filteredIndexCount,
            -- Health metrics based on usage
            COUNT(
                DISTINCT CASE
                    WHEN us.user_seeks + us.user_scans + us.user_lookups > 0
                    AND us.user_updates > 0
                    AND i.index_id > 0 THEN i.index_id
                END
            ) AS usedIndexCount,
            COUNT(
                DISTINCT CASE
                    WHEN (
                        us.user_seeks + us.user_scans + us.user_lookups = 0
                        OR us.user_seeks IS NULL
                    )
                    AND i.index_id > 0 THEN i.index_id
                END
            ) AS unusedIndexCount,
            -- Fragmentation check (simplified - would need more complex logic for full assessment)
            COUNT(
                DISTINCT CASE
                    WHEN ps.avg_fragmentation_in_percent > 30
                    AND ps.page_count > 1000
                    AND i.index_id > 0 THEN i.index_id
                END
            ) AS fragmentedIndexCount
        FROM
            sys.tables t
            LEFT JOIN sys.indexes i ON t.object_id = i.object_id
            LEFT JOIN sys.dm_db_index_usage_stats us ON i.object_id = us.object_id
            AND i.index_id = us.index_id
            AND us.database_id = DB_ID()
            OUTER APPLY sys.dm_db_index_physical_stats (DB_ID(), i.object_id, i.index_id, NULL, 'LIMITED') ps
        WHERE
            t.is_ms_shipped = 0
        GROUP BY
            t.object_id
    ),
    MissingIndexSummary AS (
        -- Get missing index information per table
        SELECT
            mid.object_id,
            COUNT(*) AS missingIndexCount,
            MAX(
                migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans)
            ) AS maxImprovementMeasure,
            SUM(migs.user_seeks + migs.user_scans) AS totalQueriesAffected,
            MAX(migs.avg_user_impact) AS maxExpectedImprovement,
            -- Count critical and high priority missing indexes
            COUNT(
                CASE
                    WHEN migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10000 THEN 1
                END
            ) AS criticalMissingCount,
            COUNT(
                CASE
                    WHEN migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 1000
                    AND migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) <= 10000 THEN 1
                END
            ) AS highMissingCount,
            COUNT(
                CASE
                    WHEN migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 100
                    AND migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) <= 1000 THEN 1
                END
            ) AS mediumMissingCount
        FROM
            sys.dm_db_missing_index_groups mig
            INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
            INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
        WHERE
            mid.database_id = DB_ID()
        GROUP BY
            mid.object_id
    ),
    TableWorkloadAnalysis AS (
        SELECT
            t.object_id,
            t.name AS tableName,
            p.row_count AS [rowCount],
            CAST(
                p.reserved_page_count * 8.0 / 1024 AS DECIMAL(10, 2)
            ) AS tableSizeMB,
            ISNULL(SUM(us.user_seeks), 0) AS userSeeks,
            ISNULL(SUM(us.user_scans), 0) AS userScans,
            ISNULL(SUM(us.user_lookups), 0) AS userLookups,
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
            END AS readWriteRatio,
            MAX(us.last_user_seek) AS lastSeek,
            MAX(us.last_user_scan) AS lastScan,
            MAX(us.last_user_lookup) AS lastLookup,
            MAX(us.last_user_update) AS lastUpdate,
            DATEDIFF(DAY, MAX(us.last_user_update), GETDATE()) AS daysSinceLastWrite,
            DATEDIFF(
                DAY,
                COALESCE(
                    MAX(us.last_user_seek),
                    MAX(us.last_user_scan),
                    MAX(us.last_user_lookup)
                ),
                GETDATE()
            ) AS daysSinceLastRead
        FROM
            sys.tables t
            INNER JOIN (
                SELECT
                    object_id,
                    SUM(row_count) AS row_count,
                    SUM(reserved_page_count) AS reserved_page_count
                FROM
                    sys.dm_db_partition_stats
                WHERE
                    index_id IN (0, 1)
                GROUP BY
                    object_id
            ) p ON t.object_id = p.object_id
            LEFT JOIN sys.dm_db_index_usage_stats us ON t.object_id = us.object_id
            AND us.database_id = DB_ID()
        WHERE
            t.is_ms_shipped = 0
        GROUP BY
            t.object_id,
            t.name,
            p.row_count,
            p.reserved_page_count
    ),
    TableNature AS (
        SELECT
            twa.*,
            pk.primaryKeyName,
            pk.indexType AS pkIndexType,
            pk.keyColumns AS pkColumns,
            pk.keyDataTypes AS pkDataTypes,
            pk.keyColumnCount AS pkColumnCount,
            pk.hasComputedKey AS pkHasComputed,
            pk.firstKeyDataType AS pkFirstDataType,
            pk.indexSizeMB AS pkSizeMB,
            pk.fill_factor AS pkFillFactor,
            pk.allow_row_locks AS pkAllowRowLocks,
            pk.allow_page_locks AS pkAllowPageLocks,
            -- Index Metrics
            ISNULL(im.totalIndexCount, 0) AS totalIndexCount,
            ISNULL(im.activeIndexCount, 0) AS activeIndexCount,
            ISNULL(im.disabledIndexCount, 0) AS disabledIndexCount,
            ISNULL(im.uniqueIndexCount, 0) AS uniqueIndexCount,
            ISNULL(im.filteredIndexCount, 0) AS filteredIndexCount,
            ISNULL(im.usedIndexCount, 0) AS usedIndexCount,
            ISNULL(im.unusedIndexCount, 0) AS unusedIndexCount,
            ISNULL(im.fragmentedIndexCount, 0) AS fragmentedIndexCount,
            -- Missing Index Metrics
            ISNULL(mis.missingIndexCount, 0) AS missingIndexCount,
            ISNULL(mis.criticalMissingCount, 0) AS criticalMissingCount,
            ISNULL(mis.highMissingCount, 0) AS highMissingCount,
            ISNULL(mis.mediumMissingCount, 0) AS mediumMissingCount,
            ISNULL(mis.maxImprovementMeasure, 0) AS maxMissingIndexImpact,
            ISNULL(mis.totalQueriesAffected, 0) AS queriesAffectedByMissing,
            -- Calculate healthy index count
            ISNULL(im.usedIndexCount, 0) - ISNULL(im.fragmentedIndexCount, 0) AS healthyIndexCount,
            -- PK Type Category
            CASE
                WHEN pk.primaryKeyName IS NULL THEN 'NO_PRIMARY_KEY'
                WHEN pk.hasIdentityKey = 1
                AND pk.keyColumnCount = 1 THEN 'IDENTITY_SINGLE'
                WHEN pk.keyColumnCount = 1
                AND pk.firstKeyDataType IN ('int', 'bigint', 'smallint', 'tinyint') THEN 'NUMERIC_SINGLE'
                WHEN pk.keyColumnCount = 1
                AND pk.firstKeyDataType = 'uniqueidentifier' THEN 'GUID_SINGLE'
                WHEN pk.keyColumnCount = 1
                AND pk.firstKeyDataType IN ('varchar', 'nvarchar', 'char', 'nchar') THEN 'STRING_SINGLE'
                WHEN pk.keyColumnCount = 1
                AND pk.firstKeyDataType IN ('date', 'datetime', 'datetime2', 'smalldatetime') THEN 'DATETIME_SINGLE'
                WHEN pk.keyColumnCount > 1 THEN 'COMPOSITE_KEY'
                ELSE 'OTHER_TYPE'
            END AS pkTypeCategory,
            -- Size Category
            CASE
                WHEN [rowCount] < 1000 THEN 'TINY'
                WHEN [rowCount] < 10000 THEN 'SMALL'
                WHEN [rowCount] < 100000 THEN 'MEDIUM'
                WHEN [rowCount] < 1000000 THEN 'LARGE'
                ELSE 'VERY_LARGE'
            END AS sizeCategory,
            -- Workload Category
            CASE
                WHEN totalReads = 0
                AND totalWrites = 0 THEN 'INACTIVE'
                WHEN totalWrites = 0
                AND totalReads > 0 THEN 'READ_ONLY'
                WHEN totalReads = 0
                AND totalWrites > 0 THEN 'WRITE_ONLY'
                WHEN readWriteRatio >= 100 THEN 'READ_INTENSIVE'
                WHEN readWriteRatio >= 10 THEN 'READ_HEAVY'
                WHEN readWriteRatio >= 3 THEN 'READ_MODERATE'
                WHEN readWriteRatio >= 0.33 THEN 'BALANCED'
                WHEN readWriteRatio >= 0.1 THEN 'WRITE_MODERATE'
                WHEN readWriteRatio >= 0.01 THEN 'WRITE_HEAVY'
                ELSE 'WRITE_INTENSIVE'
            END AS workloadCategory,
            -- Access Pattern
            CASE
                WHEN userSeeks > 0
                AND userScans = 0
                AND userLookups = 0 THEN 'SEEK_ONLY'
                WHEN userScans > 0
                AND userSeeks = 0
                AND userLookups = 0 THEN 'SCAN_ONLY'
                WHEN userLookups > (userSeeks + userScans) THEN 'LOOKUP_HEAVY'
                WHEN userSeeks > (userScans * 5) THEN 'SEEK_DOMINANT'
                WHEN userScans > (userSeeks * 5) THEN 'SCAN_DOMINANT'
                WHEN userSeeks > 0
                AND userScans > 0 THEN 'MIXED_ACCESS'
                ELSE 'NO_ACCESS'
            END AS accessPattern,
            -- Activity Level
            CASE
                WHEN totalReads + totalWrites = 0 THEN 'NO_ACTIVITY'
                WHEN totalReads + totalWrites < 100 THEN 'VERY_LOW'
                WHEN totalReads + totalWrites < 1000 THEN 'LOW'
                WHEN totalReads + totalWrites < 10000 THEN 'MODERATE'
                WHEN totalReads + totalWrites < 100000 THEN 'HIGH'
                ELSE 'VERY_HIGH'
            END AS activityLevel,
            -- Temporal Pattern
            CASE
                WHEN daysSinceLastWrite IS NULL
                AND daysSinceLastRead IS NULL THEN 'NEVER_USED'
                WHEN daysSinceLastWrite > 90
                AND daysSinceLastRead < 7 THEN 'REFERENCE_DATA'
                WHEN daysSinceLastWrite < 1
                AND daysSinceLastRead < 1 THEN 'HOT_TABLE'
                WHEN daysSinceLastWrite > 30
                AND daysSinceLastRead > 30 THEN 'COLD_TABLE'
                WHEN daysSinceLastWrite > 7
                AND daysSinceLastRead < 1 THEN 'REPORTING_TABLE'
                WHEN daysSinceLastWrite < 1
                AND daysSinceLastRead > 7 THEN 'STAGING_TABLE'
                ELSE 'REGULAR_TABLE'
            END AS temporalPattern,
            -- Data Volatility
            CASE
                WHEN totalWrites = 0
                OR [rowCount] = 0 THEN 'STATIC'
                WHEN CAST(totalWrites AS FLOAT) / NULLIF([rowCount], 0) > 10 THEN 'HIGHLY_VOLATILE'
                WHEN CAST(totalWrites AS FLOAT) / NULLIF([rowCount], 0) > 1 THEN 'VOLATILE'
                WHEN CAST(totalWrites AS FLOAT) / NULLIF([rowCount], 0) > 0.1 THEN 'MODERATE_CHANGES'
                ELSE 'STABLE'
            END AS dataVolatility
        FROM
            TableWorkloadAnalysis twa
            LEFT JOIN PrimaryKeyInfo pk ON twa.object_id = pk.object_id
            LEFT JOIN IndexMetrics im ON twa.object_id = im.object_id
            LEFT JOIN MissingIndexSummary mis ON twa.object_id = mis.object_id
    ),
    CategoryDescriptions AS (
        SELECT
            *,
            -- Index Health Assessment
            CASE
                WHEN totalIndexCount = 0
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Crititcal'
                WHEN (criticalMissingCount + highMissingCount) > 0
                AND unusedIndexCount > 2 THEN 'Poor'
                WHEN healthyIndexCount < (totalIndexCount * 0.5)
                AND totalIndexCount > 0 THEN 'Poor'
                WHEN (criticalMissingCount + highMissingCount) > 2 THEN 'Needs Attention'
                WHEN unusedIndexCount > (totalIndexCount * 0.3)
                AND totalIndexCount > 3 THEN 'Needs Review'
                WHEN fragmentedIndexCount > 2 THEN 'Needs Maintenance'
                WHEN healthyIndexCount = totalIndexCount
                AND missingIndexCount = 0 THEN 'Excellent'
                WHEN healthyIndexCount >= (totalIndexCount * 0.8)
                AND (criticalMissingCount + highMissingCount) = 0 THEN 'Good'
                ELSE 'Fair'
            END AS indexHealthStatus,
            CASE
                WHEN totalIndexCount = 0
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'No indexes on large table'
                WHEN (criticalMissingCount + highMissingCount) > 0
                AND unusedIndexCount > 2 THEN 'Missing critical indexes while having unused ones'
                WHEN healthyIndexCount < (totalIndexCount * 0.5)
                AND totalIndexCount > 0 THEN 'Over 50% of indexes are unhealthy'
                WHEN (criticalMissingCount + highMissingCount) > 2 THEN 'Multiple high-impact missing indexes'
                WHEN unusedIndexCount > (totalIndexCount * 0.3)
                AND totalIndexCount > 3 THEN 'Too many unused indexes'
                WHEN fragmentedIndexCount > 2 THEN 'Multiple fragmented indexes'
                WHEN healthyIndexCount = totalIndexCount
                AND missingIndexCount = 0 THEN 'All indexes healthy, none missing'
                WHEN healthyIndexCount >= (totalIndexCount * 0.8)
                AND (criticalMissingCount + highMissingCount) = 0 THEN 'Most indexes healthy'
                ELSE 'Some index optimization opportunities'
            END AS indexHealthStatusDescription,
            -- Access Pattern Description
            CASE accessPattern
                WHEN 'SEEK_ONLY' THEN 'Queries are efficiently finding specific rows using indexes. This is the optimal access pattern, indicating well-designed indexes that match query predicates perfectly.'
                WHEN 'SCAN_ONLY' THEN 'All queries are reading the entire table. For small tables this is acceptable, but for large tables it indicates missing indexes or queries without WHERE clauses.'
                WHEN 'LOOKUP_HEAVY' THEN 'After finding rows via index, queries frequently need additional columns from the base table. Consider creating covering indexes that include these additional columns.'
                WHEN 'SEEK_DOMINANT' THEN 'Most queries efficiently find specific rows, with occasional full table reads. This is generally a healthy pattern with good index usage.'
                WHEN 'SCAN_DOMINANT' THEN 'Most queries scan the entire table rather than seeking specific rows. Review query patterns and add appropriate indexes to convert scans to seeks.'
                WHEN 'MIXED_ACCESS' THEN 'The table serves various query types - some finding specific rows, others reading broader datasets. This versatile usage pattern requires balanced indexing.'
                WHEN 'NO_ACCESS' THEN 'No recorded access patterns. The table hasn''t been queried since the last SQL Server restart or statistics reset.'
            END AS accessPatternDescription,
            -- Temporal Pattern Description
            CASE temporalPattern
                WHEN 'NEVER_USED' THEN 'This table has never been accessed since creation or last statistics reset. Verify if it''s still needed or awaiting future use.'
                WHEN 'REFERENCE_DATA' THEN 'Classic reference/lookup pattern: frequently read (within last week) but rarely updated (not in 90+ days). Data is stable and primarily serves as information source.'
                WHEN 'HOT_TABLE' THEN 'Extremely active with both reads and writes in the last 24 hours. This is a critical operational table requiring close monitoring for performance issues.'
                WHEN 'COLD_TABLE' THEN 'No activity in over 30 days for both reads and writes. This dormant data might be historical, archived, or no longer relevant to current operations.'
                WHEN 'REPORTING_TABLE' THEN 'Read frequently (within 24 hours) but written infrequently (not in last week). Typical pattern for reporting tables, data marts, or analytical datasets.'
                WHEN 'STAGING_TABLE' THEN 'Written recently (within 24 hours) but rarely read (not in last week). Common for ETL staging areas where data lands before processing.'
                WHEN 'REGULAR_TABLE' THEN 'Shows ongoing activity with regular reads and writes. This is a normally functioning operational table.'
            END AS temporalPatternDescription,
            -- Data Volatility Description
            CASE dataVolatility
                WHEN 'STATIC' THEN 'Data never changes after insertion. Typical for log tables, historical records, or immutable audit trails. Ideal for compression and read optimization.'
                WHEN 'STABLE' THEN 'Data changes occasionally relative to table size. Most rows remain unchanged for long periods. Standard maintenance schedules are sufficient.'
                WHEN 'MODERATE_CHANGES' THEN 'Regular updates affect a moderate portion of rows. Balance between read optimization and write efficiency needed. Monitor fragmentation levels.'
                WHEN 'VOLATILE' THEN 'Significant portion of data changes frequently. High update activity relative to table size requires frequent maintenance and careful index design.'
                WHEN 'HIGHLY_VOLATILE' THEN 'Extreme change rate - data is constantly churning. Consider design alternatives like queues, in-memory tables, or different storage strategies.'
            END AS dataVolatilityDescription,
            -- Human-friendly category names
            CASE pkTypeCategory
                WHEN 'NO_PRIMARY_KEY' THEN 'No Primary Key'
                WHEN 'IDENTITY_SINGLE' THEN 'Single Identity'
                WHEN 'NUMERIC_SINGLE' THEN 'Single Numeric'
                WHEN 'GUID_SINGLE' THEN 'Single GUID'
                WHEN 'STRING_SINGLE' THEN 'Single String'
                WHEN 'DATETIME_SINGLE' THEN 'Single DateTime'
                WHEN 'COMPOSITE_KEY' THEN 'Composite Key'
                WHEN 'OTHER_TYPE' THEN 'Other Type'
            END AS pkTypeCategoryFriendly,
            CASE sizeCategory
                WHEN 'TINY' THEN 'Tiny'
                WHEN 'SMALL' THEN 'Small'
                WHEN 'MEDIUM' THEN 'Medium'
                WHEN 'LARGE' THEN 'Large'
                WHEN 'VERY_LARGE' THEN 'Very Large'
            END AS sizeCategoryFriendly,
            CASE workloadCategory
                WHEN 'INACTIVE' THEN 'Inactive'
                WHEN 'READ_ONLY' THEN 'Read Only'
                WHEN 'WRITE_ONLY' THEN 'Write Only'
                WHEN 'READ_INTENSIVE' THEN 'Read Intensive'
                WHEN 'READ_HEAVY' THEN 'Read Heavy'
                WHEN 'READ_MODERATE' THEN 'Read Moderate'
                WHEN 'BALANCED' THEN 'Balanced'
                WHEN 'WRITE_MODERATE' THEN 'Write Moderate'
                WHEN 'WRITE_HEAVY' THEN 'Write Heavy'
                WHEN 'WRITE_INTENSIVE' THEN 'Write Intensive'
            END AS workloadCategoryFriendly,
            CASE accessPattern
                WHEN 'SEEK_ONLY' THEN 'Seek Only'
                WHEN 'SCAN_ONLY' THEN 'Scan Only'
                WHEN 'LOOKUP_HEAVY' THEN 'Lookup Heavy'
                WHEN 'SEEK_DOMINANT' THEN 'Seek Dominant'
                WHEN 'SCAN_DOMINANT' THEN 'Scan Dominant'
                WHEN 'MIXED_ACCESS' THEN 'Mixed Access'
                WHEN 'NO_ACCESS' THEN 'No Access'
            END AS accessPatternFriendly,
            CASE activityLevel
                WHEN 'NO_ACTIVITY' THEN 'No Activity'
                WHEN 'VERY_LOW' THEN 'Very Low'
                WHEN 'LOW' THEN 'Low'
                WHEN 'MODERATE' THEN 'Moderate'
                WHEN 'HIGH' THEN 'High'
                WHEN 'VERY_HIGH' THEN 'Very High'
            END AS activityLevelFriendly,
            CASE temporalPattern
                WHEN 'NEVER_USED' THEN 'Never Used'
                WHEN 'REFERENCE_DATA' THEN 'Reference Data'
                WHEN 'HOT_TABLE' THEN 'Hot Table'
                WHEN 'COLD_TABLE' THEN 'Cold Table'
                WHEN 'REPORTING_TABLE' THEN 'Reporting Table'
                WHEN 'STAGING_TABLE' THEN 'Staging Table'
                WHEN 'REGULAR_TABLE' THEN 'Regular Table'
            END AS temporalPatternFriendly,
            CASE dataVolatility
                WHEN 'STATIC' THEN 'Static'
                WHEN 'STABLE' THEN 'Stable'
                WHEN 'MODERATE_CHANGES' THEN 'Moderate Changes'
                WHEN 'VOLATILE' THEN 'Volatile'
                WHEN 'HIGHLY_VOLATILE' THEN 'Highly Volatile'
            END AS dataVolatilityFriendly
        FROM
            TableNature
    ),
    FinalAnalysis AS (
        SELECT
            *,
            -- Health Status Column (simplified one-word description)
            CASE
            -- Critical issues (immediate action needed)
                WHEN totalIndexCount = 0
                AND sizeCategory IN ('LARGE', 'VERY_LARGE')
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN 'Critical'
                WHEN pkTypeCategory = 'NO_PRIMARY_KEY'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Critical'
                WHEN healthyIndexCount = 0
                AND totalIndexCount > 0 THEN 'Critical'
                WHEN sizeCategory IN ('LARGE', 'VERY_LARGE')
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN 'Critical'
                -- Poor health (significant problems)
                WHEN (criticalMissingCount + highMissingCount) > 2
                AND unusedIndexCount > 2 THEN 'Poor'
                WHEN pkTypeCategory = 'GUID_SINGLE'
                AND workloadCategory IN ('WRITE_HEAVY', 'WRITE_INTENSIVE') THEN 'Poor'
                WHEN unusedIndexCount > (totalIndexCount * 0.5)
                AND totalIndexCount > 2 THEN 'Poor'
                WHEN (criticalMissingCount + highMissingCount) > 0
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN 'Poor'
                -- Fair health (needs attention)
                WHEN pkTypeCategory = 'COMPOSITE_KEY'
                AND pkColumnCount > 3
                AND activityLevel IN ('HIGH', 'VERY_HIGH') THEN 'Fair'
                WHEN pkTypeCategory = 'STRING_SINGLE'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Fair'
                WHEN fragmentedIndexCount > 2
                AND activityLevel IN ('HIGH', 'VERY_HIGH') THEN 'Fair'
                WHEN activityLevel IN ('HIGH', 'VERY_HIGH')
                AND dataVolatility IN ('VOLATILE', 'HIGHLY_VOLATILE') THEN 'Fair'
                WHEN temporalPattern = 'COLD_TABLE'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Fair'
                WHEN workloadCategory = 'WRITE_INTENSIVE'
                AND accessPattern = 'LOOKUP_HEAVY' THEN 'Fair'
                WHEN temporalPattern = 'HOT_TABLE'
                AND sizeCategory = 'VERY_LARGE' THEN 'Fair'
                -- Good health (well-optimized)
                WHEN accessPattern = 'SEEK_ONLY'
                AND workloadCategory IN ('READ_HEAVY', 'READ_INTENSIVE')
                AND pkTypeCategory IN ('IDENTITY_SINGLE', 'NUMERIC_SINGLE')
                AND healthyIndexCount = totalIndexCount THEN 'Excellent'
                WHEN dataVolatility = 'STATIC'
                AND temporalPattern = 'REFERENCE_DATA'
                AND unusedIndexCount = 0 THEN 'Excellent'
                -- Default (acceptable)
                ELSE 'Good'
            END AS healthStatus,
            CASE
            -- Critical index issues
                WHEN totalIndexCount = 0
                AND sizeCategory IN ('LARGE', 'VERY_LARGE')
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN 'Large table with no indexes and full scans. Immediate indexing required to prevent severe performance issues.'
                WHEN (criticalMissingCount + highMissingCount) > 2
                AND unusedIndexCount > 2 THEN CAST(
                    criticalMissingCount + highMissingCount AS VARCHAR
                ) + ' critical missing indexes while maintaining ' + CAST(unusedIndexCount AS VARCHAR) + ' unused indexes. Remove unused and add missing indexes.'
                WHEN healthyIndexCount = 0
                AND totalIndexCount > 0 THEN 'All existing indexes are unhealthy (fragmented or unused). Immediate maintenance and review required.'
                -- Critical PK issues
                WHEN pkTypeCategory = 'NO_PRIMARY_KEY'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Large table without primary key. This poses serious data integrity and performance risks. Add a primary key immediately.'
                WHEN pkTypeCategory = 'GUID_SINGLE'
                AND workloadCategory IN ('WRITE_HEAVY', 'WRITE_INTENSIVE') THEN 'GUID primary key with heavy writes causes severe fragmentation. Consider sequential GUID or surrogate identity key.'
                WHEN pkTypeCategory = 'COMPOSITE_KEY'
                AND pkColumnCount > 3
                AND activityLevel IN ('HIGH', 'VERY_HIGH') THEN 'Composite key with ' + CAST(pkColumnCount AS VARCHAR) + ' columns impacts join performance in high-activity table. Consider surrogate key.'
                WHEN pkTypeCategory = 'STRING_SINGLE'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'String primary key on large table may impact performance. Evaluate if numeric surrogate would be beneficial.'
                -- Index optimization opportunities
                WHEN (criticalMissingCount + highMissingCount) > 0
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN CAST(
                    criticalMissingCount + highMissingCount AS VARCHAR
                ) + ' high-impact missing indexes causing table scans. Create recommended indexes.'
                WHEN unusedIndexCount > (totalIndexCount * 0.5)
                AND totalIndexCount > 2 THEN CAST(unusedIndexCount AS VARCHAR) + ' of ' + CAST(totalIndexCount AS VARCHAR) + ' indexes are unused. Remove to reduce maintenance overhead.'
                WHEN fragmentedIndexCount > 2
                AND activityLevel IN ('HIGH', 'VERY_HIGH') THEN CAST(fragmentedIndexCount AS VARCHAR) + ' fragmented indexes on high-activity table. Schedule immediate rebuild/reorganize.'
                -- Standard recommendations
                WHEN sizeCategory IN ('LARGE', 'VERY_LARGE')
                AND accessPattern IN ('SCAN_ONLY', 'SCAN_DOMINANT') THEN 'Large table with excessive scanning. Immediate index review required to prevent performance degradation.'
                WHEN activityLevel IN ('HIGH', 'VERY_HIGH')
                AND dataVolatility IN ('VOLATILE', 'HIGHLY_VOLATILE') THEN 'High activity with volatile data. Schedule frequent index maintenance and monitor for fragmentation.'
                WHEN temporalPattern = 'COLD_TABLE'
                AND sizeCategory IN ('LARGE', 'VERY_LARGE') THEN 'Large cold table consuming resources. Consider archiving, compression, or moving to cheaper storage.'
                WHEN workloadCategory = 'WRITE_INTENSIVE'
                AND accessPattern = 'LOOKUP_HEAVY' THEN 'Write-heavy table with many lookups. Covering indexes would help reads but hurt writes - careful balance needed.'
                WHEN temporalPattern = 'HOT_TABLE'
                AND sizeCategory = 'VERY_LARGE' THEN 'Very large hot table may need partitioning, read replicas, or application-level caching.'
                -- Positive combinations
                WHEN accessPattern = 'SEEK_ONLY'
                AND workloadCategory IN ('READ_HEAVY', 'READ_INTENSIVE')
                AND pkTypeCategory IN ('IDENTITY_SINGLE', 'NUMERIC_SINGLE')
                AND healthyIndexCount = totalIndexCount THEN 'Read-heavy table with efficient numeric PK, all indexes healthy, and seek operations. Current design is excellent.'
                WHEN dataVolatility = 'STATIC'
                AND temporalPattern = 'REFERENCE_DATA'
                AND unusedIndexCount = 0 THEN 'Perfect candidate for aggressive caching, compression, and read optimization. All indexes are being used.'
                ELSE 'Monitor standard metrics and maintain regular optimization schedule. Index health: ' + CAST(healthyIndexCount AS VARCHAR) + '/' + CAST(totalIndexCount AS VARCHAR) + ' healthy.'
            END AS healthStatusDescription
        FROM
            CategoryDescriptions
    )
SELECT
    tableName AS [Table],
    -- Primary Key Information
    ISNULL(primaryKeyName, 'NO PRIMARY KEY') AS [PK Name],
    ISNULL(pkIndexType, 'N/A') AS [PK Index Type],
    ISNULL(pkColumns, 'N/A') AS [PK Columns],
    ISNULL(pkDataTypes, 'N/A') AS [PK Data Types],
    CASE
        WHEN pkFillFactor IS NULL
        OR pkFillFactor = 0 THEN 100
        ELSE pkFillFactor
    END AS [PK Fill Factor (%)],
    -- Table Size Metrics
    FORMAT([rowCount], 'N0') AS [Row Count],
    CASE
        WHEN tableSizeMB >= 1048576 THEN FORMAT(tableSizeMB / 1048576.0, 'N2') + ' TB'
        WHEN tableSizeMB >= 1024 THEN FORMAT(tableSizeMB / 1024.0, 'N2') + ' GB'
        ELSE FORMAT(tableSizeMB, 'N2') + ' MB'
    END AS [Table Size],
    -- Categories with descriptions (using friendly names)
    sizeCategoryFriendly AS [Size Category],
    -- Index Information (NEW)
    totalIndexCount AS [Total Indices],
    healthyIndexCount AS [Healthy Indices],
    CASE
        WHEN (criticalMissingCount + highMissingCount) > 0 THEN CAST(
            criticalMissingCount + highMissingCount AS VARCHAR
        ) + ' (' + CAST(criticalMissingCount AS VARCHAR) + ' critical, ' + CAST(highMissingCount AS VARCHAR) + ' high)'
        ELSE '0'
    END AS [Missing Indices (High/Critical)],
    unusedIndexCount AS [Unused Indices],
    fragmentedIndexCount AS [Fragmented Indices],
    indexHealthStatus AS [Index Health Status],
    -- indexHealthStatusDescription as [Index Health Status Description],
    CASE
        WHEN workloadCategory = 'INACTIVE' THEN 'No Activity'
        WHEN workloadCategory = 'READ_ONLY' THEN 'No Writes (Read-Only)'
        WHEN workloadCategory = 'WRITE_ONLY' THEN 'No Reads (Write-Only)'
        WHEN readWriteRatio = 999999 THEN 'No Writes (Read-Only)'
        WHEN readWriteRatio >= 100 THEN 'Read-Intensive (' + FORMAT(readWriteRatio, 'N0') + ':1)'
        WHEN readWriteRatio >= 10 THEN 'Read-Heavy (' + FORMAT(readWriteRatio, 'N0') + ':1)'
        WHEN readWriteRatio >= 3 THEN 'Read-Moderate (' + FORMAT(readWriteRatio, 'N0') + ':1)'
        WHEN readWriteRatio >= 1 THEN 'Balanced (' + FORMAT(readWriteRatio, 'N0') + ':1)'
        WHEN readWriteRatio >= 0.33 THEN 'Balanced (1:' + FORMAT(1.0 / readWriteRatio, 'N0') + ')'
        WHEN readWriteRatio >= 0.1 THEN 'Write-Moderate (1:' + FORMAT(1.0 / readWriteRatio, 'N0') + ')'
        WHEN readWriteRatio >= 0.01 THEN 'Write-Heavy (1:' + FORMAT(1.0 / readWriteRatio, 'N0') + ')'
        WHEN readWriteRatio > 0 THEN 'Write-Intensive (1:' + FORMAT(1.0 / readWriteRatio, 'N0') + ')'
        ELSE 'Very Write-Heavy (1:' + FORMAT(1.0 / readWriteRatio, 'N0') + ')'
    END AS [Read/Write Pattern],
    accessPatternFriendly AS [Access Pattern],
    -- accessPatternDescription AS [Access Pattern Description],
    -- activityLevelFriendly AS [Activity Level],
    -- temporalPatternDescription AS [Activity Level Description],
    -- dataVolatilityFriendly AS [Data Volatility],
    -- dataVolatilityDescription AS [Data Volatility Description],
    healthStatus AS [Health Status]
    -- healthStatusDescription AS [Health Status Description]
FROM
    FinalAnalysis
WHERE 
    activityLevelFriendly <> 'No Activity';`;

  const result = await db.executeQuery({ query });
  if (!result || result.length === 0) {
    return 'No active tables found in the schema.';
  }
  return JSON.stringify(result, null, 2);
}
