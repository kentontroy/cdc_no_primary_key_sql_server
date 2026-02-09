-- https://devblogs.microsoft.com/azure-sql/change-tracking-in-azure-sql-database/

-- ALTER DATABASE "sql-db-0408286" SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 5 DAYS, AUTO_CLEANUP = ON); 
-- ALTER TABLE dbo.multiset_state ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);  
-- ALTER TABLE dbo.multiset_state DISABLE CHANGE_TRACKING;

CREATE OR ALTER PROCEDURE dbo.enable_change_tracking_for_table
(
    @table_name nvarchar(512),      -- 'schema.table' or 'table'
    @retention_days int = 7,
    @auto_cleanup bit = 1,
    @track_columns_updated bit = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @schema_name sysname = COALESCE(PARSENAME(@table_name, 2), N'dbo'),
        @object_name sysname = PARSENAME(@table_name, 1);

    IF @object_name IS NULL
        THROW 50000, 'table_name is required.', 1;

    -- Validate table exists
    IF OBJECT_ID(QUOTENAME(@schema_name) + '.' + QUOTENAME(@object_name), 'U') IS NULL
        THROW 50000, 'Table not found in current database.', 1;

    -- Enable DB-level Change Tracking if needed
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.change_tracking_databases
        WHERE database_id = DB_ID()
    )
    BEGIN
        DECLARE @stmt nvarchar(max) =
            N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) +
            N' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = ' +
            CAST(@retention_days AS nvarchar(20)) +
            N' DAYS, AUTO_CLEANUP = ' +
            CASE WHEN @auto_cleanup = 1 THEN N'ON' ELSE N'OFF' END +
            N');';

        EXEC(@stmt);
    END;

    -- Enable table-level Change Tracking if needed
    IF NOT EXISTS
    (
        SELECT 1
        FROM sys.change_tracking_tables
        WHERE object_id = OBJECT_ID(QUOTENAME(@schema_name) + '.' + QUOTENAME(@object_name))
    )
    BEGIN
        DECLARE @stmt2 nvarchar(max) =
            N'ALTER TABLE ' + QUOTENAME(@schema_name) + '.' + QUOTENAME(@object_name) +
            N' ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ' +
            CASE WHEN @track_columns_updated = 1 THEN N'ON' ELSE N'OFF' END +
            N');';

        EXEC(@stmt2);
    END;
END;
GO
