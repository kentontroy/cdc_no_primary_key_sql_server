-- https://devblogs.microsoft.com/azure-sql/change-tracking-in-azure-sql-database/
ALTER DATABASE "sql-db-0408286" SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 5 DAYS, AUTO_CLEANUP = ON); 
ALTER TABLE dbo.cdc_multiset_state ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);  

DECLARE @LastSyncVersion BIGINT =
  CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID('dbo.cdc_multiset_state'));

SELECT CT.*
FROM CHANGETABLE(CHANGES dbo.cdc_multiset_state, @LastSyncVersion) AS CT;   

GO

