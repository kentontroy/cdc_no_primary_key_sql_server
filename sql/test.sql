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