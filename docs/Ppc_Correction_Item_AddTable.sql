-- Additive: Ppc_Correction_Item — Phase 3 of NDT Input Slit SAP status tracking
-- (docs/NDT_Input_Slit_SAP_Status_Design.md). When an operator's slit reconcile touches data whose
-- output file is already SAP-Accepted, the local (MES) correction is applied as usual and one row
-- is auto-created here with the facts PPC needs to apply the same change manually in SAP
-- (bundle / file / slit / old value / corrected value). Nothing is sent automatically; the
-- operator clears the item after PPC confirms the SAP-side fix.
-- "Ppc_Correction_Pending" on a bundle is DERIVED: any row with Status = 'Open' for that
-- NDT_Batch_No. There is no flag column on NDT_Bundle, so the status can never drift.
-- Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Ppc_Correction_Item' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Ppc_Correction_Item (
        Ppc_Correction_Item_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        NDT_Batch_No           NVARCHAR(50)   NOT NULL,
        File_Name              NVARCHAR(260)  NOT NULL,  -- SAP-Accepted output basename the correction refers to
        Slit_No                NVARCHAR(50)   NOT NULL,  -- normalized slit key (empty slit stored as N'—')
        Old_NDT_Pipes          INT            NULL,      -- slit value at the time of the first local correction (= what SAP still has)
        Corrected_NDT_Pipes    INT            NOT NULL,  -- latest locally applied value (updated if reconciled again while Open)
        Status                 NVARCHAR(16)   NOT NULL CONSTRAINT DF_Ppc_Correction_Item_Status DEFAULT (N'Open'),  -- Open | Cleared
        Created_AtUtc          DATETIME2(3)   NOT NULL CONSTRAINT DF_Ppc_Correction_Item_Created DEFAULT (SYSUTCDATETIME()),
        Updated_AtUtc          DATETIME2(3)   NULL,      -- last Corrected_NDT_Pipes change while Open
        Cleared_AtUtc          DATETIME2(3)   NULL,
        Cleared_By             NVARCHAR(128)  NULL,
        Cleared_Note           NVARCHAR(512)  NULL
    );

    -- One Open item per (bundle, file, slit): repeated local corrections update Corrected_NDT_Pipes
    -- on the existing Open row instead of stacking duplicates.
    CREATE UNIQUE INDEX UX_Ppc_Correction_Item_OpenKey
        ON dbo.Ppc_Correction_Item (NDT_Batch_No, File_Name, Slit_No)
        WHERE Status = N'Open';

    CREATE INDEX IX_Ppc_Correction_Item_Batch
        ON dbo.Ppc_Correction_Item (NDT_Batch_No, Status);
END
GO
