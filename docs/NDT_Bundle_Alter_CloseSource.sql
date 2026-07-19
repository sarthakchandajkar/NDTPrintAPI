-- Additive: PLC close / CSV recon columns for Phase 3 F-2.
-- Run against JazeeraMES_Prod (or Dev) before deploying NdtBundleService Phase 3 F-2.
-- Safe to re-run: columns added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Close_Source') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Close_Source NVARCHAR(20) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Awaiting_Csv_Recon') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Awaiting_Csv_Recon BIT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Awaiting_Csv_Recon DEFAULT (0);
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Count_Discrepancy') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Count_Discrepancy BIT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Count_Discrepancy DEFAULT (0);
GO
