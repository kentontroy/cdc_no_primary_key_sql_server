CREATE OR ALTER PROCEDURE dbo.disable_change_tracking_for_table
(
    @table_name nvarchar(512),           -- 'schema.table' or 'table'
    @disable_db_if_unused bit = 0        -- set to 1 to disable DB CT if no CT tables remain
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @schema_name sysname = COALESCE(PARSENAME(@table_name, 2), N'dbo'),
        @object_name sysname = PARSENAME(@table_name, 1);

    IF @object_name IS NULL OR LTRIM(RTRIM(@object_name)) = N''
        THROW 50000, 'table_name is required (table or schema.table).', 1;

    DECLARE @full_name nvarchar(600) = QUOTENAME(@schema_name) + N'.' + QUOTENAME(@object_name);

    -- Validate table exists
    IF OBJECT_ID(@full_name, 'U') IS NULL
        THROW 50000, 'Table not found in current database.', 1;

    -- Disable table-level Change Tracking if enabled
    IF EXISTS
    (
        SELECT 1
        FROM sys.change_tracking_tables
        WHERE object_id = OBJECT_ID(@full_name, 'U')
    )
    BEGIN
        DECLARE @stmt nvarchar(max) =
            N'ALTER TABLE ' + @full_name + N' DISABLE CHANGE_TRACKING;';

        EXEC (@stmt);
    END;

    -- Optionally disable DB-level Change Tracking if no CT tables remain
    IF @disable_db_if_unused = 1
       AND EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID())
       AND NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables)
    BEGIN
        DECLARE @stmt2 nvarchar(max) =
            N'ALTER DATABASE ' + QUOTENAME(DB_NAME()) + N' SET CHANGE_TRACKING = OFF;';

        EXEC (@stmt2);
    END;
END;
GO
