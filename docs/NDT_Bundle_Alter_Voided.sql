-- Additive: voided-bundle tombstone for operator merge (keeps unique Bundle_No + FK).
-- After moving Output_Slit_Row to the target, source Bundle_No is renamed to {original}V
-- so the live number can be reused. Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Voided') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Voided BIT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Voided DEFAULT (0);
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Voided_AtUtc') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Voided_AtUtc DATETIME2(3) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Voided_Reason') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Voided_Reason NVARCHAR(512) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Original_Bundle_No') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Original_Bundle_No NVARCHAR(20) NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Merged_Into_Bundle_No') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Merged_Into_Bundle_No NVARCHAR(20) NULL;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_NDT_Bundle_Voided' AND object_id = OBJECT_ID(N'dbo.NDT_Bundle'))
    CREATE INDEX IX_NDT_Bundle_Voided ON dbo.NDT_Bundle (Voided, Mill_No)
        WHERE Voided = 1;
GO

-- Extend fill-state CHECK so a voided source cannot remain incomplete.
IF EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE name = N'CK_NDT_Bundle_Csv_Fill_State'
      AND parent_object_id = OBJECT_ID(N'dbo.NDT_Bundle'))
    ALTER TABLE dbo.NDT_Bundle DROP CONSTRAINT CK_NDT_Bundle_Csv_Fill_State;
GO

ALTER TABLE dbo.NDT_Bundle WITH NOCHECK
ADD CONSTRAINT CK_NDT_Bundle_Csv_Fill_State
CHECK (Csv_Fill_State IN (
    N'PlcClosed',
    N'CsvFilling',
    N'CsvComplete',
    N'CsvShort',
    N'CsvOvershoot',
    N'Voided'));
GO
