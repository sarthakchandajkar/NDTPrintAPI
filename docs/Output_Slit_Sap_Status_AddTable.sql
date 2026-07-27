-- Additive: Output_Slit_Sap_Status + Output_Slit_Sap_Status_Event — SAP lifecycle status per
-- NDT Input Slit output CSV basename (Pending → Accepted | Rejected → Pending on resubmit).
-- Filenames do not change when SAP moves files between folders, so File_Name (basename, no path)
-- is the durable key. Accepted is terminal/frozen: rows never leave Accepted (see
-- docs/NDT_Input_Slit_SAP_Status_Design.md). The Event table is append-only audit and is REQUIRED
-- (not optional) per design review 2026-07-27.
-- Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Output_Slit_Sap_Status' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Output_Slit_Sap_Status (
        Output_Slit_Sap_Status_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        File_Name                 NVARCHAR(260)  NOT NULL,  -- basename only, e.g. 2604361_01_260726_1000060363.csv
        Status                    NVARCHAR(16)   NOT NULL,  -- Pending | Accepted | Rejected
        Status_AtUtc              DATETIME2(3)   NOT NULL CONSTRAINT DF_Output_Slit_Sap_Status_AtUtc DEFAULT (SYSUTCDATETIME()),
        File_LastWriteTimeUtc     DATETIME2(3)   NULL,      -- last known version of the file
        Observed_Folder           NVARCHAR(512)  NULL,      -- folder that produced the current status
        Prior_Status              NVARCHAR(16)   NULL,
        Resubmit_Count            INT            NOT NULL CONSTRAINT DF_Output_Slit_Sap_Status_Resubmit DEFAULT (0)
    );

    CREATE UNIQUE INDEX UX_Output_Slit_Sap_Status_File
        ON dbo.Output_Slit_Sap_Status (File_Name);

    CREATE INDEX IX_Output_Slit_Sap_Status_Status
        ON dbo.Output_Slit_Sap_Status (Status);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Output_Slit_Sap_Status_Event' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Output_Slit_Sap_Status_Event (
        Output_Slit_Sap_Status_Event_ID BIGINT        IDENTITY(1,1) NOT NULL PRIMARY KEY,
        File_Name                 NVARCHAR(260)  NOT NULL,
        Prior_Status              NVARCHAR(16)   NULL,      -- NULL on first observation
        New_Status                NVARCHAR(16)   NOT NULL,
        Event_Type                NVARCHAR(32)   NOT NULL,  -- Initial | Transition | Resubmitted | RegressionIgnored
        Observed_Folder           NVARCHAR(512)  NULL,
        File_LastWriteTimeUtc     DATETIME2(3)   NULL,
        Source                    NVARCHAR(32)   NOT NULL,  -- Watcher | SeedOnWrite
        Event_AtUtc               DATETIME2(3)   NOT NULL CONSTRAINT DF_Output_Slit_Sap_Status_Event_AtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Output_Slit_Sap_Status_Event_File
        ON dbo.Output_Slit_Sap_Status_Event (File_Name, Event_AtUtc);
END
GO
