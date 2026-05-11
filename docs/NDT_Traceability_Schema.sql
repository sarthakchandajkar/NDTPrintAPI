-- =============================================================================
-- NDT Bundle traceability schema (SQL Server)
-- Run in SSMS against the target database, e.g.:
--   Dev:  USE JazeeraMES_Dev;
--   Prod: USE JazeeraMES_Prod;
-- Creates: NDT_Bundle, Input_Slit_Row, Output_Slit_Row, PO_Plan_WIP, Formation_Chart,
--          Bundle_Label, Manual_Station_Run, Upload_Bundle_Row, Pipeline_Event, NDT_Process_Consolidated
-- Idempotent: skips tables that already exist.
-- Note: docs/NDT_Process_Consolidated_AddTable.sql is redundant if you run this full script (same table).
-- =============================================================================

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- -----------------------------------------------------------------------------
-- 1. NDT_Bundle — completed bundles / reconciliation (matches app + NDT_Bundle_Table.sql)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'NDT_Bundle' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.NDT_Bundle (
        NDTBundle_ID              BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Mill_No                   INT            NOT NULL,
        Bundle_No                 NVARCHAR(20)   NOT NULL,
        Total_NDT_Pcs             INT            NOT NULL,
        Context_Slit_No           NVARCHAR(50)   NULL,
        Slit_Start_Time           DATETIME2(2)   NULL,
        Slit_Finish_Time          DATETIME2(2)  NULL,
        Rejected_P                INT            NOT NULL CONSTRAINT DF_NDT_Bundle_Rejected_P DEFAULT (0),
        NDT_Short_Length_Pipe     NVARCHAR(50)   NULL,
        Rejected_Short_Length_Pipe NVARCHAR(50)  NULL,
        PrintedAt                 DATETIME2(2)   NOT NULL CONSTRAINT DF_NDT_Bundle_PrintedAt DEFAULT (SYSDATETIME()),
        IsReprint                 BIT            NOT NULL CONSTRAINT DF_NDT_Bundle_IsReprint DEFAULT (0)
    );

    CREATE UNIQUE INDEX UQ_NDT_Bundle_Bundle_No ON dbo.NDT_Bundle (Bundle_No);
    CREATE INDEX IX_NDT_Bundle_PO_Mill ON dbo.NDT_Bundle (PO_Number, Mill_No);
END
GO

-- -----------------------------------------------------------------------------
-- 2. Input_Slit_Row — lines from input slit (SAP) CSVs
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Input_Slit_Row' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Input_Slit_Row (
        Input_Slit_Row_ID         BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Slit_No                   NVARCHAR(50)   NULL,
        NDT_Pipes                 INT            NOT NULL CONSTRAINT DF_Input_Slit_NDT_Pipes DEFAULT (0),
        Rejected_P                INT            NOT NULL CONSTRAINT DF_Input_Slit_Rejected_P DEFAULT (0),
        Slit_Start_Time           DATETIME2(2)   NULL,
        Slit_Finish_Time          DATETIME2(2)  NULL,
        Mill_No                   INT            NULL,
        NDT_Short_Length_Pipe     NVARCHAR(50)   NULL,
        Rejected_Short_Length_Pipe NVARCHAR(50)  NULL,
        Source_File               NVARCHAR(500)  NULL,
        Source_Row_Number         INT            NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL CONSTRAINT DF_Input_Slit_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Input_Slit_Row_PO ON dbo.Input_Slit_Row (PO_Number);
    CREATE INDEX IX_Input_Slit_Row_Imported ON dbo.Input_Slit_Row (ImportedAtUtc);
END
GO

-- -----------------------------------------------------------------------------
-- 3. Output_Slit_Row — per-slit output CSV rows (input columns + NDT Batch No)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Output_Slit_Row' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Output_Slit_Row (
        Output_Slit_Row_ID        BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Slit_No                   NVARCHAR(50)   NULL,
        NDT_Pipes                 INT            NOT NULL CONSTRAINT DF_Output_Slit_NDT_Pipes DEFAULT (0),
        Rejected_P                INT            NOT NULL CONSTRAINT DF_Output_Slit_Rejected_P DEFAULT (0),
        Slit_Start_Time           DATETIME2(2)   NULL,
        Slit_Finish_Time          DATETIME2(2)  NULL,
        Mill_No                   INT            NULL,
        NDT_Short_Length_Pipe     NVARCHAR(50)   NULL,
        Rejected_Short_Length_Pipe NVARCHAR(50)  NULL,
        NDT_Batch_No              NVARCHAR(20)   NOT NULL,
        Source_File               NVARCHAR(500)  NULL,
        Source_Row_Number         INT            NULL,
        WrittenAtUtc              DATETIME2(2)   NOT NULL CONSTRAINT DF_Output_Slit_WrittenAtUtc DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT FK_Output_Slit_Row_Bundle FOREIGN KEY (NDT_Batch_No)
            REFERENCES dbo.NDT_Bundle (Bundle_No)
    );

    CREATE INDEX IX_Output_Slit_Row_Batch ON dbo.Output_Slit_Row (NDT_Batch_No);
    CREATE INDEX IX_Output_Slit_Row_PO ON dbo.Output_Slit_Row (PO_Number);
END
GO

-- -----------------------------------------------------------------------------
-- 4. PO_Plan_WIP — PO plan / WIP CSV snapshot (wide row for TM/WIP columns)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PO_Plan_WIP' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.PO_Plan_WIP (
        PO_Plan_WIP_ID            BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Mill_No                   INT            NULL,
        Planned_Month             NVARCHAR(50)   NULL,
        Pipe_Grade                NVARCHAR(100)  NULL,
        Pipe_Size                 NVARCHAR(50)   NULL,
        Pipe_Length               NVARCHAR(50)   NULL,
        Pieces_Per_Bundle         INT            NULL,
        NDTPcsPerBundle           INT            NULL,
        Total_Pieces              NVARCHAR(50)   NULL,
        Source_File               NVARCHAR(500)  NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL CONSTRAINT DF_PO_Plan_WIP_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_PO_Plan_WIP_PO ON dbo.PO_Plan_WIP (PO_Number);
    CREATE INDEX IX_PO_Plan_WIP_PO_Mill ON dbo.PO_Plan_WIP (PO_Number, Mill_No);
END
GO

-- -----------------------------------------------------------------------------
-- 5. Formation_Chart — pipe size → required NDT pcs per bundle
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Formation_Chart' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Formation_Chart (
        Formation_Chart_ID        BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Pipe_Size                 NVARCHAR(50)   NOT NULL,
        Required_Ndt_Pcs          INT            NOT NULL,
        CONSTRAINT UQ_Formation_Chart_Pipe_Size UNIQUE (Pipe_Size)
    );
END
GO

-- -----------------------------------------------------------------------------
-- 6. Bundle_Label — label fields keyed by (PO, Mill)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Bundle_Label' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Bundle_Label (
        Bundle_Label_ID           BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Mill_No                   INT            NOT NULL,
        Specification             NVARCHAR(200)  NULL,
        Type                      NVARCHAR(200)  NULL,
        Pipe_Size                 NVARCHAR(50)   NULL,
        Length                    NVARCHAR(50)   NULL,
        CONSTRAINT UQ_Bundle_Label_PO_Mill UNIQUE (PO_Number, Mill_No)
    );
END
GO

-- -----------------------------------------------------------------------------
-- 7. Manual_Station_Run — Visual / Hydro / Revisual station CSV rows
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Manual_Station_Run' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Manual_Station_Run (
        Manual_Station_Run_ID     BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        NDT_Batch_No              NVARCHAR(20)   NOT NULL,
        NDT_Pcs                   INT            NOT NULL,
        OK_Pcs                    INT            NOT NULL,
        Reject_Pcs                INT            NOT NULL,
        Work_Station              NVARCHAR(100)  NULL,
        Bundle_Start              DATETIME2(2)   NOT NULL,
        Bundle_End                DATETIME2(2)   NOT NULL,
        Hydrotesting_Type         NVARCHAR(100)  NULL,
        Source_File               NVARCHAR(500)  NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL CONSTRAINT DF_Manual_Station_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Manual_Station_Batch ON dbo.Manual_Station_Run (NDT_Batch_No);
    CREATE INDEX IX_Manual_Station_PO ON dbo.Manual_Station_Run (PO_Number);
END
GO

-- -----------------------------------------------------------------------------
-- 8. Upload_Bundle_Row — Upload_NDT_Bundle_*.csv layout
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Upload_Bundle_Row' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Upload_Bundle_Row (
        Upload_Bundle_Row_ID      BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_NO                     NVARCHAR(30)   NOT NULL,
        Slit_No                   NVARCHAR(50)   NULL,
        HRC_Number                NVARCHAR(50)   NULL,
        Slit_Width                NVARCHAR(50)   NULL,
        Slit_Thick                NVARCHAR(50)   NULL,
        NSS                       NVARCHAR(50)   NULL,
        Slit_Grade                NVARCHAR(100)  NULL,
        Bundle_Number             NVARCHAR(20)   NOT NULL,
        NumOfPipes                INT            NOT NULL,
        TotalBundleWt             NVARCHAR(50)   NULL,
        LenPerPipe                NVARCHAR(50)   NULL,
        IsFullBundle              BIT            NULL,
        Source_File               NVARCHAR(500)  NULL,
        GeneratedAtUtc            DATETIME2(2)   NOT NULL CONSTRAINT DF_Upload_Bundle_GeneratedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Upload_Bundle_Number ON dbo.Upload_Bundle_Row (Bundle_Number);
    CREATE INDEX IX_Upload_Bundle_PO ON dbo.Upload_Bundle_Row (PO_NO);
END
GO

-- -----------------------------------------------------------------------------
-- 9. Pipeline_Event — cross-cutting step log (not a CSV clone)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'Pipeline_Event' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.Pipeline_Event (
        Pipeline_Event_ID         BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Step_Code                 NVARCHAR(50)   NOT NULL,
        OccurredAtUtc             DATETIME2(2)   NOT NULL CONSTRAINT DF_Pipeline_Event_OccurredAtUtc DEFAULT (SYSUTCDATETIME()),
        PO_Number                 NVARCHAR(30)   NULL,
        Mill_No                   INT            NULL,
        Bundle_No                 NVARCHAR(20)   NULL,
        Slit_No                   NVARCHAR(50)   NULL,
        Correlation_Id            UNIQUEIDENTIFIER NULL,
        PayloadJson               NVARCHAR(MAX)  NULL
    );

    CREATE INDEX IX_Pipeline_Event_Bundle ON dbo.Pipeline_Event (Bundle_No);
    CREATE INDEX IX_Pipeline_Event_Time ON dbo.Pipeline_Event (OccurredAtUtc);
    CREATE INDEX IX_Pipeline_Event_Step ON dbo.Pipeline_Event (Step_Code, OccurredAtUtc);
END
GO

-- -----------------------------------------------------------------------------
-- 10. NDT_Process_Consolidated — one row per NDT process CSV (after Revisual)
--     Matches file columns: PO, batch, NDT Pcs, OK, Visual/Hydro/Revisual rejects, bundle start/end.
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'NDT_Process_Consolidated' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE dbo.NDT_Process_Consolidated (
        NDT_Process_Consolidated_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        NDT_Batch_No              NVARCHAR(20)   NOT NULL,
        NDT_Pcs                   INT            NOT NULL,
        OK_Pcs                    INT            NOT NULL,
        Visual_Reject             INT            NOT NULL,
        Hydrotest_Reject          INT            NOT NULL,
        Revisual_Reject           INT            NOT NULL,
        Bundle_Start              DATETIME2(2)   NOT NULL,
        Bundle_End                DATETIME2(2)   NOT NULL,
        Output_File               NVARCHAR(500)  NULL,
        CreatedAtUtc              DATETIME2(2)   NOT NULL CONSTRAINT DF_NDT_Process_Consolidated_CreatedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_NDT_Process_Consolidated_Batch ON dbo.NDT_Process_Consolidated (NDT_Batch_No);
    CREATE INDEX IX_NDT_Process_Consolidated_PO ON dbo.NDT_Process_Consolidated (PO_Number);
END
GO
