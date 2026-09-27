-- Additive: mill-published running SAP PO for Shared dashboard / ActivePo.
-- Mill-n fills these from local WIP running-PO state on the same ~500ms publish loop.
-- Shared prefers fresh Running_Po / Waiting_For_New_Wip over slit CSV scans.
-- Safe to re-run: columns added only if missing.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH(N'dbo.Mill_Instance_Status', N'Running_Po') IS NULL
BEGIN
    ALTER TABLE dbo.Mill_Instance_Status
        ADD Running_Po NVARCHAR(32) NULL;
END
GO

IF COL_LENGTH(N'dbo.Mill_Instance_Status', N'Waiting_For_New_Wip') IS NULL
BEGIN
    ALTER TABLE dbo.Mill_Instance_Status
        ADD Waiting_For_New_Wip BIT NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_WaitingWip DEFAULT (0);
END
GO

IF COL_LENGTH(N'dbo.Mill_Instance_Status', N'Running_Po_Source') IS NULL
BEGIN
    ALTER TABLE dbo.Mill_Instance_Status
        ADD Running_Po_Source NVARCHAR(32) NULL;
END
GO

IF COL_LENGTH(N'dbo.Mill_Instance_Status', N'Running_Po_Updated_AtUtc') IS NULL
BEGIN
    ALTER TABLE dbo.Mill_Instance_Status
        ADD Running_Po_Updated_AtUtc DATETIME2(3) NULL;
END
GO
