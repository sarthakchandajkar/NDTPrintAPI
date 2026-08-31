-- Additive: Shared-only station ZPL printers (Visual/Revisual, Big Hydro, Four-Head Hydro).
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the station-printer binary.
-- Safe to re-run: table created only if missing; seed inserts only missing station rows.
-- Seed matches today's office test printer (192.168.0.125:9100) on all three stations.
-- Also adds Print_Status / Print_Error on Manual_Station_Run (station tags never write NDT_Bundle.Print_Status).

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Station_Printer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Station_Printer
    (
        Station_Code   NVARCHAR(32)   NOT NULL
            CONSTRAINT PK_Station_Printer PRIMARY KEY
            CONSTRAINT CK_Station_Printer_Code CHECK (Station_Code IN (
                N'VISUAL_REVISUAL', N'BIG_HYDRO', N'FOUR_HEAD_HYDRO')),
        Address        NVARCHAR(128)  NOT NULL,
        Port           INT            NOT NULL
            CONSTRAINT CK_Station_Printer_Port CHECK (Port BETWEEN 1 AND 65535),
        Updated_AtUtc  DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Station_Printer_Updated DEFAULT (SYSUTCDATETIME()),
        Updated_By     NVARCHAR(128)  NULL
    );
END
GO

INSERT INTO dbo.Station_Printer (Station_Code, Address, Port, Updated_By)
SELECT s.Station_Code, N'192.168.0.125', 9100, N'Seed'
FROM (VALUES
    (N'VISUAL_REVISUAL'),
    (N'BIG_HYDRO'),
    (N'FOUR_HEAD_HYDRO')
) AS s(Station_Code)
WHERE NOT EXISTS (SELECT 1 FROM dbo.Station_Printer p WHERE p.Station_Code = s.Station_Code);
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
