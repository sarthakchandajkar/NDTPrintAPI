-- Additive: operator manual bundle reconcile (decoupled from slit CSV rows).
-- Run against JazeeraMES_Prod (or Dev) before deploying manual bundle reconcile.
-- Safe to re-run: columns added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Recon') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Recon BIT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Manual_Recon DEFAULT (0);
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Recon_By') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Recon_By NVARCHAR(100) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Recon_At') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Recon_At DATETIME2 NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Recon_Reason') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Recon_Reason NVARCHAR(500) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Manual_Recon_Original_Total') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Manual_Recon_Original_Total INT NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Post_Recon_Csv_Sum') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Post_Recon_Csv_Sum INT NULL;
GO
