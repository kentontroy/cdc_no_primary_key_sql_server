/*
This procedure:
takes @capture_instance
reads the corresponding cdc.<capture_instance>_CT table
builds canonical JSON from captured columns
aggregates deltas by signature within the LSN window
merges into the generic state table
advances watermark
*/

CREATE OR ALTER PROCEDURE dbo.apply_cdc_multiset_generic
  @capture_instance sysname
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @from_lsn binary(10);
  DECLARE @to_lsn   binary(10) = sys.fn_cdc_get_max_lsn();

  -- Resolve CT table: cdc.<capture_instance>_CT
  DECLARE @ct_table sysname = QUOTENAME(N'cdc') + N'.' + QUOTENAME(@capture_instance + N'_CT');
  DECLARE @ct_object_id int = OBJECT_ID(N'cdc.' + @capture_instance + N'_CT');

  IF @ct_object_id IS NULL
    THROW 50010, 'CDC change table for capture instance not found: cdc.<capture_instance>_CT', 1;

  -- Watermark
  SELECT @from_lsn = last_lsn
  FROM dbo.cdc_multiset_watermark
  WHERE capture_instance = @capture_instance;

  IF @from_lsn IS NULL
  BEGIN
    SET @from_lsn = sys.fn_cdc_get_min_lsn(@capture_instance);

    IF NOT EXISTS (SELECT 1 FROM dbo.cdc_multiset_watermark WHERE capture_instance = @capture_instance)
      INSERT INTO dbo.cdc_multiset_watermark(capture_instance, last_lsn)
      VALUES (@capture_instance, @from_lsn);
  END

  IF @to_lsn IS NULL OR @to_lsn <= @from_lsn
    RETURN;

  --------------------------------------------------------------------
  -- Build deterministic JSON projection from CT-table columns
  -- (exclude CDC metadata columns starting with '__$').
  --------------------------------------------------------------------
  DECLARE @json_select nvarchar(max);

  ;WITH cols AS (
    SELECT
      c.name AS column_name,
      t.name AS type_name
    FROM sys.columns c
    JOIN sys.types t
      ON c.user_type_id = t.user_type_id
    WHERE c.object_id = @ct_object_id
      AND c.name NOT LIKE N'__$%'          -- exclude CDC metadata columns
  )
  SELECT @json_select =
    STRING_AGG(expr, N',') WITHIN GROUP (ORDER BY column_name)
  FROM (
    SELECT
      column_name,
      CASE
        WHEN type_name IN (N'date',N'datetime',N'datetime2',N'smalldatetime',N'datetimeoffset',N'time')
          THEN N'CONVERT(nvarchar(33),' + QUOTENAME(column_name) + N',126) AS ' + QUOTENAME(column_name)

        WHEN type_name = N'uniqueidentifier'
          THEN N'CONVERT(nvarchar(36),' + QUOTENAME(column_name) + N') AS ' + QUOTENAME(column_name)

        WHEN type_name IN (N'binary',N'varbinary',N'image',N'timestamp',N'rowversion')
          THEN N'master.dbo.fn_varbintohexstr(CONVERT(varbinary(max),' + QUOTENAME(column_name) + N')) AS ' + QUOTENAME(column_name)

        ELSE
          -- Leave most types as-is so JSON keeps numbers/bits numeric.
          QUOTENAME(column_name) + N' AS ' + QUOTENAME(column_name)
      END AS expr
    FROM cols
  ) x;

  IF @json_select IS NULL OR LEN(@json_select) = 0
    THROW 50011, 'No captured columns found in CT table (after excluding __$ columns).', 1;

  --------------------------------------------------------------------
  -- Apply multiset deltas (duplicate-safe via row_count)
  --------------------------------------------------------------------
  DECLARE @sql nvarchar(max) = N'
;WITH c AS (
  SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CASE WHEN __$operation IN (2,4) THEN 1 ELSE -1 END AS delta,
    (
      SELECT ' + @json_select + N'
      FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES
    ) AS row_json
  FROM ' + @ct_table + N'
  WHERE __$start_lsn >  @from_lsn
    AND __$start_lsn <= @to_lsn
    AND __$operation IN (1,2,3,4)
),
d AS (
  SELECT
    HASHBYTES(''SHA2_256'', row_json) AS row_sig,
    row_json,
    SUM(CAST(delta AS bigint)) AS delta_count,
    MAX(__$start_lsn) AS last_lsn,
    MAX(__$seqval)   AS last_seq
  FROM c
  GROUP BY row_json
  HAVING SUM(CAST(delta AS bigint)) <> 0
)
MERGE dbo.cdc_multiset_state AS tgt
USING d AS src
  ON tgt.capture_instance = @capture_instance
 AND tgt.row_sig = src.row_sig
WHEN MATCHED THEN
  UPDATE SET
    tgt.row_count = tgt.row_count + src.delta_count,
    tgt.row_json  = src.row_json,
    tgt.last_lsn  = CASE WHEN src.last_lsn > ISNULL(tgt.last_lsn, 0x0) THEN src.last_lsn ELSE tgt.last_lsn END,
    tgt.last_seq  = CASE WHEN src.last_seq > ISNULL(tgt.last_seq, 0x0) THEN src.last_seq ELSE tgt.last_seq END
WHEN NOT MATCHED THEN
  INSERT (capture_instance, row_sig, row_json, row_count, last_lsn, last_seq)
  VALUES (@capture_instance, src.row_sig, src.row_json, src.delta_count, src.last_lsn, src.last_seq);

-- Drop signatures that net to zero (multiset semantics)
DELETE FROM dbo.cdc_multiset_state
WHERE capture_instance = @capture_instance
  AND row_count <= 0;

-- Advance watermark
UPDATE dbo.cdc_multiset_watermark
SET last_lsn = @to_lsn
WHERE capture_instance = @capture_instance;
';

  EXEC sys.sp_executesql
    @sql,
    N'@capture_instance sysname, @from_lsn binary(10), @to_lsn binary(10)',
    @capture_instance = @capture_instance,
    @from_lsn = @from_lsn,
    @to_lsn = @to_lsn;
END;
GO
