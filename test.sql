TRUNCATE TABLE dbo.cdc_multiset_state;

UPDATE dbo.cdc_multiset_watermark
SET last_lsn = sys.fn_cdc_get_min_lsn(N'SalesLT_Product'),
    last_seq = 0x00000000000000000000,
    last_op  = 0
WHERE capture_instance = N'SalesLT_Product';

GO