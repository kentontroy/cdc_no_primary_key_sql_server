/*
Watermark behind CT → apply processes rows → state changes → watermark advances.
Watermark at CT max → apply does nothing → state doesn’t change.
You clear state but don’t rewind watermark → state stays empty/missing rows forever 
(because apply won’t re-read already-watermarked CT rows).
*/ 
EXEC dbo.apply_cdc_multiset_generic @capture_instance = N'SalesLT_Product';
GO