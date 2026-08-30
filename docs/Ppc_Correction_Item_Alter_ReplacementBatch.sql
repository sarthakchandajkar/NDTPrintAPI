-- Additive: Replacement_NDT_Batch_No on Ppc_Correction_Item for bundle-merge key changes.
-- SAP delete-and-reload uses NDT_Batch_No (old key) then insert under Replacement.
-- Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.Ppc_Correction_Item', 'Replacement_NDT_Batch_No') IS NULL
    ALTER TABLE dbo.Ppc_Correction_Item ADD Replacement_NDT_Batch_No NVARCHAR(50) NULL;
GO
