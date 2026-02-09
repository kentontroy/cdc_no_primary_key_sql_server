SET IMPLICIT_TRANSACTIONS OFF;

SELECT t2.name AS table_name, c.name AS column_name, t.name AS data_type, c.max_length, c.is_nullable
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.tables t2 ON c.object_id = t2.object_id
WHERE t2.name = 'test_sales_events'; 

BEGIN TRANSACTION;

INSERT INTO dbo.test_sales_events (sku, region, channel, event_type, ts, price)
VALUES (
    CONCAT(
        'SKU-TEST-',
        RIGHT(
            CONCAT('0000', ABS(CHECKSUM(NEWID())) % 10000),
            4
        )
    ),
    'US-W',
    'PARTNER',
    'PRICE_SET',
    DATEADD(DAY, 1, CAST(GETDATE() AS date)),
    100.00
);

COMMIT TRANSACTION;
GO

SELECT @@TRANCOUNT;