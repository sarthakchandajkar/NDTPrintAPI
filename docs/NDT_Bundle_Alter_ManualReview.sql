-- Additive: Manual_Review flag for F-5.2 backfill / F-4 orphan policy.
-- Run against JazeeraMES_Prod (or Dev) before deploying NdtBundleService Phase 2.
-- Safe to re-run: column added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Review') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Review BIT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Manual_Review DEFAULT (0);
GO
