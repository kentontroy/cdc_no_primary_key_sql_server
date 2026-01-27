/*
This procedure:
takes @capture_instance
reads the corresponding cdc.<capture_instance>_CT table
builds canonical JSON from captured columns
aggregates deltas by signature within the LSN window
merges into the generic state table
advances watermark
*/

/*
Azure SQL Server supports CONVERT(varchar(max), <varbinary>, 2) which returns a hex string (no 0x prefix).
It’s widely available and doesn’t require master.
So replace:
master.dbo.fn_varbintohexstr(CONVERT(varbinary(max), [col]))
with:
CONVERT(varchar(max), CONVERT(varbinary(max), [col]), 2)
*/

/*===============================================================
  Updated apply proc: watermark by (LSN, SEQVAL, OP)
     - Prevents skipping op=4 that appears later with same LSN/SEQ
     - Caps @to to max tuple present in CT table (instance-specific)
  ===============================================================*/
CREATE OR ALTER PROCEDURE dbo.apply_cdc_multiset_generic
@capture_instance sysname
AS
BEGIN
  SET NOCOUNT ON;
  --------------------------------------------------------------------
  -- Resolve CT table
  --------------------------------------------------------------------
  DECLARE @ct_object_id int = OBJECT_ID(N'cdc.' + @capture_instance + N'_CT');
  IF @ct_object_id IS NULL
    THROW 50010, 'CDC change table for capture instance not found: cdc.<capture_instance>_CT', 1;

  DECLARE @ct_table nvarchar(300) =
      QUOTENAME(N'cdc') + N'.' + QUOTENAME(@capture_instance + N'_CT');

  --------------------------------------------------------------------
  -- Load or initialize watermark (LSN, SEQVAL, OP)
  --------------------------------------------------------------------
  DECLARE @from_lsn binary(10);
  DECLARE @from_seq binary(10);
  DECLARE @from_op  int;

  SELECT
    @from_lsn = last_lsn,
    @from_seq = last_seq,
    @from_op  = last_op
  FROM dbo.cdc_multiset_watermark
  WHERE capture_instance = @capture_instance;

  IF @from_seq IS NULL SET @from_seq = 0x00000000000000000000;
  IF @from_op  IS NULL SET @from_op  = 0;

  IF @from_lsn IS NULL
  BEGIN
    SET @from_lsn = sys.fn_cdc_get_min_lsn(@capture_instance);

    IF EXISTS (SELECT 1 FROM dbo.cdc_multiset_watermark WHERE capture_instance = @capture_instance)
    BEGIN
      UPDATE dbo.cdc_multiset_watermark
      SET last_lsn = @from_lsn,
          last_seq = 0x00000000000000000000,
          last_op  = 0
      WHERE capture_instance = @capture_instance;
    END
    ELSE
    BEGIN
      INSERT INTO dbo.cdc_multiset_watermark(capture_instance, last_lsn, last_seq, last_op)
      VALUES (@capture_instance, @from_lsn, 0x00000000000000000000, 0);
    END

    SET @from_seq = 0x00000000000000000000;
    SET @from_op  = 0;
  END

  --------------------------------------------------------------------
  -- Compute @to tuple as the MAX tuple currently present in CT table,
  -- but only among rows strictly greater than the current watermark.
  -- If none exist, nothing to do.
  --------------------------------------------------------------------
  DECLARE @to_lsn binary(10);
  DECLARE @to_seq binary(10);
  DECLARE @to_op  int;

  DECLARE @sql_to nvarchar(max) = N'
SELECT TOP (1)
  @to_lsn_out = __$start_lsn,
  @to_seq_out = __$seqval,
  @to_op_out  = __$operation
FROM ' + @ct_table + N'
WHERE __$operation IN (1,2,3,4)
  AND (
        __$start_lsn > @from_lsn
     OR (__$start_lsn = @from_lsn AND __$seqval > @from_seq)
     OR (__$start_lsn = @from_lsn AND __$seqval = @from_seq AND __$operation > @from_op)
      )
ORDER BY __$start_lsn DESC, __$seqval DESC, __$operation DESC;';

  EXEC sys.sp_executesql
    @sql_to,
    N'@from_lsn binary(10), @from_seq binary(10), @from_op int,
      @to_lsn_out binary(10) OUTPUT, @to_seq_out binary(10) OUTPUT, @to_op_out int OUTPUT',
    @from_lsn = @from_lsn,
    @from_seq = @from_seq,
    @from_op  = @from_op,
    @to_lsn_out = @to_lsn OUTPUT,
    @to_seq_out = @to_seq OUTPUT,
    @to_op_out  = @to_op  OUTPUT;

  IF @to_lsn IS NULL
    RETURN;

  --------------------------------------------------------------------
  -- Build deterministic JSON projection from CT table columns
  -- excluding CDC metadata columns (__$*)
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
      AND c.name NOT LIKE N'__$%'
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
          THEN N'CONVERT(varchar(max), CONVERT(varbinary(max),' + QUOTENAME(column_name) + N'), 2) AS ' + QUOTENAME(column_name)

        ELSE
          QUOTENAME(column_name) + N' AS ' + QUOTENAME(column_name)
      END AS expr
    FROM cols
  ) x;

  IF @json_select IS NULL OR LEN(@json_select) = 0
    THROW 50011, 'No captured columns found in CT table (after excluding __$ columns).', 1;

  --------------------------------------------------------------------
  -- Apply: select CT rows in (from, to] by tuple comparison,
  -- compute per-row signature, aggregate deltas, merge into state,
  -- then advance watermark to @to tuple.
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
  WHERE
    (
          __$start_lsn > @from_lsn
       OR (__$start_lsn = @from_lsn AND __$seqval > @from_seq)
       OR (__$start_lsn = @from_lsn AND __$seqval = @from_seq AND __$operation > @from_op)
    )
    AND
    (
          __$start_lsn < @to_lsn
       OR (__$start_lsn = @to_lsn AND __$seqval < @to_seq)
       OR (__$start_lsn = @to_lsn AND __$seqval = @to_seq AND __$operation <= @to_op)
    )
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

-- Remove signatures that net to zero
UPDATE dbo.cdc_multiset_state
SET is_deleted = 1,
    row_count = 0
WHERE capture_instance = @capture_instance
  AND row_count <= 0
  AND is_deleted = 0;
/*
This was replaced by SQL Update above
DELETE FROM dbo.cdc_multiset_state
WHERE capture_instance = @capture_instance
  AND row_count <= 0;
*/

-- Advance watermark to the @to tuple we just processed through
UPDATE dbo.cdc_multiset_watermark
SET last_lsn = @to_lsn,
    last_seq = @to_seq,
    last_op  = @to_op
WHERE capture_instance = @capture_instance;
';

  EXEC sys.sp_executesql
    @sql,
    N'@capture_instance sysname,
      @from_lsn binary(10), @from_seq binary(10), @from_op int,
      @to_lsn binary(10),   @to_seq binary(10),   @to_op int',
    @capture_instance = @capture_instance,
    @from_lsn = @from_lsn, @from_seq = @from_seq, @from_op = @from_op,
    @to_lsn   = @to_lsn,   @to_seq   = @to_seq,   @to_op   = @to_op;
END;
GO

/*
In Snowflake:
MERGE INTO SALESLT_PRODUCT_STATE t
USING STAGE_BATCH s
ON t.ROW_SIG = s.ROW_SIG
WHEN MATCHED AND s.IS_DELETED THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET
    ROW_JSON  = s.ROW_JSON,
    ROW_COUNT = s.ROW_COUNT,
    LAST_LSN  = s.LAST_LSN,
    LAST_SEQ  = s.LAST_SEQ,
    IS_DELETED = s.IS_DELETED,
    STATE_RV  = s.STATE_RV
WHEN NOT MATCHED AND NOT s.IS_DELETED THEN
  INSERT (ROW_SIG, ROW_JSON, ROW_COUNT, LAST_LSN, LAST_SEQ, IS_DELETED, STATE_RV)
  VALUES (s.ROW_SIG, s.ROW_JSON, s.ROW_COUNT, s.LAST_LSN, s.LAST_SEQ, s.IS_DELETED, s.STATE_RV);
*/
