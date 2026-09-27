-- Forward-only: allow Output_Slit_Row.NDT_Batch_No NULL so Constant / zero-NDT / hollow-FG
-- CSV values (e.g. 10001) are written to disk only — no NDT_Bundle parent, no FK violation.
-- SQL Server FK allows NULL child keys (NULL does not need a matching parent).
-- Does NOT remediate existing Bundle_No='10001' NDT_Bundle rows.

IF COL_LENGTH('dbo.Output_Slit_Row', 'NDT_Batch_No') IS NOT NULL
   AND EXISTS (
       SELECT 1
       FROM sys.columns c
       WHERE c.object_id = OBJECT_ID(N'dbo.Output_Slit_Row')
         AND c.name = N'NDT_Batch_No'
         AND c.is_nullable = 0)
BEGIN
    ALTER TABLE dbo.Output_Slit_Row
        ALTER COLUMN NDT_Batch_No NVARCHAR(20) NULL;
END
GO
