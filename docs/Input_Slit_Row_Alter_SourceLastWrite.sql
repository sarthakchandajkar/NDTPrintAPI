-- Additive: Source_LastWriteTimeUtc + Source_File index for F-5 backfill version checks.
-- Run against JazeeraMES_Prod (or Dev) before deploying NdtBundleService Phase 2.
-- Safe to re-run: column/index added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.Input_Slit_Row', 'Source_LastWriteTimeUtc') IS NULL
    ALTER TABLE dbo.Input_Slit_Row ADD Source_LastWriteTimeUtc DATETIME2(2) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Input_Slit_Row_Source_File'
      AND object_id = OBJECT_ID(N'dbo.Input_Slit_Row'))
BEGIN
    CREATE INDEX IX_Input_Slit_Row_Source_File
        ON dbo.Input_Slit_Row (Source_File)
        WHERE Source_File IS NOT NULL;
END
GO
