-- Additive: fill-to-target CSV assignment columns.
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the fill-to-target binary.
-- Safe to re-run: columns / constraint added only if missing.
-- Quiet-drain deploy: ensure Awaiting_Csv_Recon = 0 for all rows before cutover.

USE JazeeraMES_Prod;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Target_Ndt_Pcs') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Target_Ndt_Pcs INT NULL;
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Csv_Filled') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Csv_Filled INT NOT NULL
        CONSTRAINT DF_NDT_Bundle_Csv_Filled DEFAULT (0);
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Csv_Fill_State') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Csv_Fill_State NVARCHAR(32) NOT NULL
        CONSTRAINT DF_NDT_Bundle_Csv_Fill_State DEFAULT (N'PlcClosed');
GO

IF COL_LENGTH('dbo.NDT_Bundle', 'Csv_Last_Row_AtUtc') IS NULL
    ALTER TABLE dbo.NDT_Bundle ADD Csv_Last_Row_AtUtc DATETIME2(3) NULL;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.check_constraints
    WHERE name = N'CK_NDT_Bundle_Csv_Fill_State'
      AND parent_object_id = OBJECT_ID(N'dbo.NDT_Bundle'))
BEGIN
    ALTER TABLE dbo.NDT_Bundle WITH NOCHECK
    ADD CONSTRAINT CK_NDT_Bundle_Csv_Fill_State
    CHECK (Csv_Fill_State IN (
        N'PlcClosed',
        N'CsvFilling',
        N'CsvComplete',
        N'CsvShort',
        N'CsvOvershoot'));
END
GO

-- Held slit files awaiting a fill target (no invented batch number).
IF OBJECT_ID(N'dbo.NDT_Csv_Fill_Hold', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.NDT_Csv_Fill_Hold
    (
        Hold_ID            BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        Source_File_Name   NVARCHAR(260) NOT NULL,
        PO_Number          NVARCHAR(32) NOT NULL,
        Mill_No            INT NOT NULL,
        Pipe_Size          NVARCHAR(32) NULL,
        Held_AtUtc         DATETIME2(3) NOT NULL
            CONSTRAINT DF_NDT_Csv_Fill_Hold_Held_AtUtc DEFAULT (SYSUTCDATETIME()),
        Reason_Code        NVARCHAR(64) NOT NULL,
        Manual_Review      BIT NOT NULL
            CONSTRAINT DF_NDT_Csv_Fill_Hold_Manual_Review DEFAULT (0),
        CONSTRAINT UQ_NDT_Csv_Fill_Hold_Source UNIQUE (Source_File_Name)
    );
END
GO

-- Audit for operator / resubmit batch moves (one correlation ID per atomic move).
IF OBJECT_ID(N'dbo.NDT_Csv_Fill_Audit', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.NDT_Csv_Fill_Audit
    (
        Audit_ID           BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        Correlation_Id     UNIQUEIDENTIFIER NOT NULL,
        Event_Type         NVARCHAR(64) NOT NULL,
        Source_File_Name   NVARCHAR(260) NULL,
        Old_Batch_No       NVARCHAR(32) NULL,
        New_Batch_No       NVARCHAR(32) NULL,
        Pipe_Delta         INT NULL,
        Detail_Json        NVARCHAR(MAX) NULL,
        AtUtc              DATETIME2(3) NOT NULL
            CONSTRAINT DF_NDT_Csv_Fill_Audit_AtUtc DEFAULT (SYSUTCDATETIME())
    );
    CREATE INDEX IX_NDT_Csv_Fill_Audit_Correlation
        ON dbo.NDT_Csv_Fill_Audit (Correlation_Id, AtUtc);
END
GO

-- Backfill Target for existing printed rows so cutover does not refuse start on historical closes.
UPDATE dbo.NDT_Bundle
SET Target_Ndt_Pcs = Total_NDT_Pcs,
    Csv_Filled = COALESCE(Csv_Filled, 0),
    Csv_Fill_State = CASE
        WHEN Csv_Fill_State IS NULL OR LTRIM(RTRIM(Csv_Fill_State)) = N'' THEN N'CsvComplete'
        ELSE Csv_Fill_State
    END
WHERE Target_Ndt_Pcs IS NULL
  AND Total_NDT_Pcs > 0;
GO
