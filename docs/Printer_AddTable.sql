-- Additive: one ZPL printer table for mills 1–4 and station printers (Visual/Revisual, Big Hydro, Four-Head Hydro).
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying a binary that reads dbo.Printer.
-- Safe to re-run: table created only if missing; copies leftover Mill_Printer / Station_Printer rows;
-- seed inserts only missing keys; drops the two old tables after copy.
-- Seed matches the office test printer (192.168.0.125:9100) on all seven keys.
-- Also adds Print_Status / Print_Error on Manual_Station_Run (station tags never write NDT_Bundle.Print_Status).

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Printer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Printer
    (
        Printer_Key    NVARCHAR(32)   NOT NULL
            CONSTRAINT PK_Printer PRIMARY KEY,
        Kind           NVARCHAR(16)   NOT NULL
            CONSTRAINT CK_Printer_Kind CHECK (Kind IN (N'Mill', N'Station')),
        Address        NVARCHAR(128)  NOT NULL,
        Port           INT            NOT NULL
            CONSTRAINT CK_Printer_Port CHECK (Port BETWEEN 1 AND 65535),
        Updated_AtUtc  DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Printer_Updated DEFAULT (SYSUTCDATETIME()),
        Updated_By     NVARCHAR(128)  NULL,
        CONSTRAINT CK_Printer_Key CHECK (
            (Kind = N'Mill' AND Printer_Key IN (N'MILL_1', N'MILL_2', N'MILL_3', N'MILL_4'))
            OR
            (Kind = N'Station' AND Printer_Key IN (
                N'VISUAL_REVISUAL', N'BIG_HYDRO', N'FOUR_HEAD_HYDRO')))
    );
END
GO

-- Copy mill rows if the old table is still present (first cutover only).
IF OBJECT_ID(N'dbo.Mill_Printer', N'U') IS NOT NULL
BEGIN
    INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_AtUtc, Updated_By)
    SELECT
        N'MILL_' + CONVERT(NVARCHAR(2), p.Mill_No),
        N'Mill',
        p.Address,
        p.Port,
        p.Updated_AtUtc,
        p.Updated_By
    FROM dbo.Mill_Printer p
    WHERE p.Mill_No BETWEEN 1 AND 4
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.Printer t
          WHERE t.Printer_Key = N'MILL_' + CONVERT(NVARCHAR(2), p.Mill_No));
END
GO

-- Copy station rows if the old table is still present (first cutover only).
IF OBJECT_ID(N'dbo.Station_Printer', N'U') IS NOT NULL
BEGIN
    INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_AtUtc, Updated_By)
    SELECT
        p.Station_Code,
        N'Station',
        p.Address,
        p.Port,
        p.Updated_AtUtc,
        p.Updated_By
    FROM dbo.Station_Printer p
    WHERE p.Station_Code IN (N'VISUAL_REVISUAL', N'BIG_HYDRO', N'FOUR_HEAD_HYDRO')
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.Printer t
          WHERE t.Printer_Key = p.Station_Code);
END
GO

INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_By)
SELECT s.Printer_Key, s.Kind, N'192.168.0.125', 9100, N'Seed'
FROM (VALUES
    (N'MILL_1', N'Mill'),
    (N'MILL_2', N'Mill'),
    (N'MILL_3', N'Mill'),
    (N'MILL_4', N'Mill'),
    (N'VISUAL_REVISUAL', N'Station'),
    (N'BIG_HYDRO', N'Station'),
    (N'FOUR_HEAD_HYDRO', N'Station')
) AS s(Printer_Key, Kind)
WHERE NOT EXISTS (SELECT 1 FROM dbo.Printer p WHERE p.Printer_Key = s.Printer_Key);
GO

IF OBJECT_ID(N'dbo.Mill_Printer', N'U') IS NOT NULL
    DROP TABLE dbo.Mill_Printer;
GO

IF OBJECT_ID(N'dbo.Station_Printer', N'U') IS NOT NULL
    DROP TABLE dbo.Station_Printer;
GO

IF COL_LENGTH(N'dbo.Manual_Station_Run', N'Print_Status') IS NULL
BEGIN
    ALTER TABLE dbo.Manual_Station_Run
        ADD Print_Status NVARCHAR(32) NULL;
END
GO

IF COL_LENGTH(N'dbo.Manual_Station_Run', N'Print_Error') IS NULL
BEGIN
    ALTER TABLE dbo.Manual_Station_Run
        ADD Print_Error NVARCHAR(400) NULL;
END
GO
