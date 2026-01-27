SELECT COUNT(1) FROM dbo.cdc_test_sales_events;

SELECT TOP (10) *
FROM dbo.cdc_test_sales_events
ORDER BY NEWID();

SET NOCOUNT ON;

SELECT 
  col_number,
  col_ts,
  JSON_VALUE(col_text,'$.sku') AS sku,
  JSON_VALUE(col_text,'$.region') AS region,
  JSON_VALUE(col_text,'$.channel') AS channel,
  JSON_VALUE(col_text,'$.event_type') AS event_type,
  JSON_VALUE(col_text,'$.batch') AS batch,
  JSON_VALUE(col_text,'$.dup_group') AS dup_group
FROM dbo.cdc_test_sales_events 
WHERE JSON_VALUE(col_text,'$.sku') = 'SKU-0076'
AND JSON_VALUE(col_text,'$.region') = 'US-W'
AND JSON_VALUE(col_text,'$.channel') = 'PARTNER';

SELECT
  DB_NAME() AS db,
  @@SPID    AS spid,
  @@TRANCOUNT AS trancount,
  SESSIONPROPERTY('IMPLICIT_TRANSACTIONS') AS implicit_tran;
 /*
 <schema>_<table_name>_CT
 Keeping a history of all changes
 https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-capture-instance-ct-transact-sql?view=sql-server-ver17
 __$operation == 1 = delete
 __$operation == 2 = insert
 __$operation == 3 = update (old values)
 __$operation == 4 = update (new values)
*/

EXEC sys.sp_cdc_help_jobs;

EXEC sys.sp_cdc_help_change_data_capture
    @source_schema = N'dbo',
    @source_name   = N'cdc_test_sales_events';

SELECT TOP(200) 
  __$start_lsn, __$seqval, __$operation,
  s.col_number,
  s.col_ts,
  JSON_VALUE(s.col_text,'$.sku') AS sku,
  JSON_VALUE(s.col_text,'$.region') AS region,
  JSON_VALUE(s.col_text,'$.channel') AS channel,
  JSON_VALUE(s.col_text,'$.event_type') AS event_type,
  JSON_VALUE(s.col_text,'$.batch') AS batch,
  JSON_VALUE(s.col_text,'$.dup_group') AS dup_group
FROM cdc.dbo_cdc_test_sales_events_CT s
WHERE JSON_VALUE(s.col_text,'$.sku') = 'SKU-0076'
ORDER BY __$start_lsn DESC, __$seqval DESC;

SELECT capture_instance, state_rv, state_rv_bigint, row_count, row_json, last_lsn, last_seq
FROM dbo.cdc_multiset_state
WHERE capture_instance = N'dbo_cdc_test_sales_events'
ORDER BY state_rv_bigint DESC;

GO
