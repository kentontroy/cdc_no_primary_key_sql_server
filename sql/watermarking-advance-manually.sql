/*
Watermark behind CT → apply processes rows → state changes → watermark advances.
Watermark at CT max → apply does nothing → state doesn’t change.
You clear state but don’t rewind watermark → state stays empty/missing rows forever 
(because apply won’t re-read already-watermarked CT rows).
*/ 
EXEC dbo.apply_cdc_multiset_generic @capture_instance = N'dbo_cdc_test_sales_events';
GO

/*
How the apply proc works (simple mental model)
Think of it as a CDC consumer that repeatedly does four steps:
Read the watermark
From dbo.cdc_multiset_watermark it gets the last processed position:
(last_lsn, last_seq, last_op)

Find what’s new in _CT
It looks in cdc.<capture_instance>_CT for rows whose tuple
( __$start_lsn, __$seqval, __$operation ) is greater than the watermark.

Apply deltas to the multiset state
For each CT row:
op 2 (INSERT) and op 4 (after-update) ⇒ +1
op 1 (DELETE) and op 3 (before-update) ⇒ -1

It computes a signature for the full row image (your JSON + SHA256), then updates
dbo.cdc_multiset_state:
add the delta to row_count for that signature
delete entries whose row_count drops to 0

Advance the watermark
Finally it updates the watermark to the “latest CT position processed” (the max tuple in that run).
That’s what makes the process incremental and prevents reprocessing.
That’s it: poll → compute deltas → merge into state → advance watermark.

Conceptually: row_json is produced for both, and op-code decides whether that JSON is being “removed” or “added”.
Apply deltas
signature(hash(row_json_before)) gets −1
signature(hash(row_json_after)) gets +1
So if state previously had the “41” signature with count 1, after applying it becomes:
“41” count → 0 (then deleted)
“42” count → 1

cdc_multiset_state will only contain row_json values that came from __$operation = 2 (INSERT) or 4 (UPDATE after-image)
So you infer “this came from op=4” only because it survived the delta replay — not because it was explicitly tagged as op=4.
Yes — after all deltas are applied, the surviving row_json values in cdc_multiset_state correspond to images last introduced 
by CDC operations 2 or 4.

*/