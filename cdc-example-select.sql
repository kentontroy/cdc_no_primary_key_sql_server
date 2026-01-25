SELECT COUNT(1) FROM SalesLT.Product;

SET ROWCOUNT 1000;

SELECT ListPrice 
FROM SalesLT.Product
WHERE ProductID = 707;

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
    @source_schema = N'SalesLT',
    @source_name   = N'Product';

SELECT TOP (200)
  __$start_lsn, __$seqval, __$operation,
  ProductID, Name, ListPrice, ModifiedDate
FROM cdc.SalesLT_Product_CT
WHERE ProductID = 707
ORDER BY __$start_lsn DESC, __$seqval DESC;

SELECT row_count, row_json, last_lsn, last_seq
FROM dbo.cdc_multiset_state
WHERE capture_instance = N'SalesLT_Product'
ORDER BY row_count DESC;

SELECT
  s.row_count,
  JSON_VALUE(s.row_json,'$.ProductID')  AS ProductID,
  JSON_VALUE(s.row_json,'$.ListPrice')  AS ListPrice,
  s.last_lsn, s.last_seq
FROM dbo.cdc_multiset_state s
WHERE s.capture_instance = N'SalesLT_Product'
  AND JSON_VALUE(s.row_json,'$.ProductID') = CONVERT(nvarchar(50), 707)
ORDER BY s.last_lsn DESC, s.last_seq DESC;

GO

