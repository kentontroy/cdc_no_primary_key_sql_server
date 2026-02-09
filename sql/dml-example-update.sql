-- https://devblogs.microsoft.com/azure-sql/change-tracking-in-azure-sql-database/

SET IMPLICIT_TRANSACTIONS OFF;

BEGIN TRANSACTION;

UPDATE dbo.test_sales_events
SET ts = DATEADD(DAY, 1, CURRENT_DATE)
WHERE sku = 'SKU-0076'
AND region = 'US-W'
AND channel = 'PARTNER';

COMMIT TRANSACTION;
GO

SELECT
  DB_NAME() AS db,
  @@SPID    AS spid,
  @@TRANCOUNT AS trancount,
  SESSIONPROPERTY('IMPLICIT_TRANSACTIONS') AS implicit_tran;

SELECT *
FROM dbo.test_sales_events
WHERE sku = 'SKU-0076'
AND region = 'US-W'
AND channel = 'PARTNER';

-- if you don't care about preserving the test update:
-- IF @@TRANCOUNT > 0 ROLLBACK;
-- SELECT @@TRANCOUNT AS trancount;

SELECT
  s.session_id,
  s.login_name,
  s.host_name,
  s.program_name,
  s.status,
  s.open_transaction_count,
  r.command,
  r.status AS request_status,
  r.wait_type,
  r.wait_time,
  r.blocking_session_id,
  DB_NAME(COALESCE(r.database_id, s.database_id)) AS db_name
FROM sys.dm_exec_sessions s
LEFT JOIN sys.dm_exec_requests r
  ON r.session_id = s.session_id
WHERE s.session_id = 78;

SELECT
  tl.request_session_id AS session_id,
  tl.resource_type,
  tl.resource_description,
  tl.request_mode,
  tl.request_status,
  tl.resource_database_id,
  DB_NAME(tl.resource_database_id) AS db_name,
  OBJECT_NAME(p.object_id, tl.resource_database_id) AS object_name,
  i.name AS index_name
FROM sys.dm_tran_locks tl
LEFT JOIN sys.partitions p
  ON p.hobt_id = tl.resource_associated_entity_id
LEFT JOIN sys.indexes i
  ON i.object_id = p.object_id AND i.index_id = p.index_id
WHERE tl.request_session_id = 78
ORDER BY tl.resource_type, tl.request_mode;
