-- Add print-status tracking columns to dbo.NDT_Bundle.
-- Run against JazeeraMES_Prod before deploying Phase 5 NdtBundleService.
-- Safe to re-run: each column/index is added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Print_Status') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Print_Status NVARCHAR(20) NOT NULL
        CONSTRAINT DF_NDT_Bundle_Print_Status DEFAULT ('Pending');
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Print_Attempted_At') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Print_Attempted_At DATETIME2(2) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Print_Error') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Print_Error NVARCHAR(500) NULL;
GO

-- Backfill: historical tag-print rows treated as already printed
UPDATE dbo.NDT_Bundle
SET Print_Status = 'Printed'
WHERE Total_NDT_Pcs > 0 AND Print_Status = 'Pending';
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_NDT_Bundle_Print_Status_Attempted' AND object_id = OBJECT_ID('dbo.NDT_Bundle'))
    CREATE INDEX IX_NDT_Bundle_Print_Status_Attempted
        ON dbo.NDT_Bundle (Print_Status, Print_Attempted_At)
        WHERE Print_Status IN ('Pending', 'PrintFailed');
GO
