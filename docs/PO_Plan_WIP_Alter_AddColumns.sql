-- Extend dbo.PO_Plan_WIP for full PO Accepted / WIP CSV columns (Pipe Thickness, weight, type, SAP item fields).
-- Run against JazeeraMES_Prod before deploying the updated NdtBundleService import.
-- Safe to re-run: each column is added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Pipe_Thickness') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Pipe_Thickness NVARCHAR(50) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Pipe_Weight_Per_Meter') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Pipe_Weight_Per_Meter NVARCHAR(50) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Pipe_Type') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Pipe_Type NVARCHAR(50) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Output_Itemcode') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Output_Itemcode NVARCHAR(50) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Item_Description') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Item_Description NVARCHAR(500) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Product_Type') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Product_Type NVARCHAR(100) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'PO_Specification') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD PO_Specification NVARCHAR(500) NULL;
GO

IF COL_LENGTH('dbo.PO_Plan_WIP', 'Input_WIP_Itemcode') IS NULL
    ALTER TABLE dbo.PO_Plan_WIP ADD Input_WIP_Itemcode NVARCHAR(50) NULL;
GO

-- After altering, re-import PO Accepted files so new columns populate:
--   1) Deploy updated service, OR
--   2) Delete rows from PO_Plan_WIP where Source_File matches the folder files you need refreshed, OR
--   3) Touch (re-save) changed CSVs so LastWriteTimeUtc changes and import keys differ.
