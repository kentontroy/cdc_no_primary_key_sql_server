-- watermark
SELECT last_lsn, last_seq, last_op
FROM dbo.cdc_multiset_watermark
WHERE capture_instance = N'SalesLT_Product';

-- op=4 tuple you care about
SELECT TOP (1) __$start_lsn, __$seqval, __$operation
FROM cdc.SalesLT_Product_CT
WHERE ProductID = 707 AND __$operation = 4
ORDER BY __$start_lsn DESC, __$seqval DESC;