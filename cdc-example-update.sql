-- https://devblogs.microsoft.com/azure-sql/change-tracking-in-azure-sql-database/

BEGIN TRANSACTION;
UPDATE SalesLT.Product
SET ListPrice = ListPrice + 1
WHERE ProductID = 707;

UPDATE SalesLT.Product
SET ListPrice = ListPrice + 1
WHERE ProductID = 707;
COMMIT TRANSACTION;

GO