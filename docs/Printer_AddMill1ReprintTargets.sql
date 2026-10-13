-- Additive: Mill-1 WIP / FG printers + PAS Room reprint printer.
-- Run against JazeeraMES_Prod (or Dev) before/with deploying multi-printer reprint.
-- Safe to re-run.

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Printer', N'U') IS NULL
BEGIN
    RAISERROR(N'dbo.Printer is missing. Run docs/Printer_AddTable.sql first.', 16, 1);
    RETURN;
END
GO

IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_Printer_Kind' AND parent_object_id = OBJECT_ID(N'dbo.Printer'))
    ALTER TABLE dbo.Printer DROP CONSTRAINT CK_Printer_Kind;
GO

IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = N'CK_Printer_Key' AND parent_object_id = OBJECT_ID(N'dbo.Printer'))
    ALTER TABLE dbo.Printer DROP CONSTRAINT CK_Printer_Key;
GO

ALTER TABLE dbo.Printer WITH CHECK ADD CONSTRAINT CK_Printer_Kind
    CHECK (Kind IN (N'Mill', N'Station', N'PasRoom'));
GO

ALTER TABLE dbo.Printer WITH CHECK ADD CONSTRAINT CK_Printer_Key CHECK (
    (Kind = N'Mill' AND Printer_Key IN (
        N'MILL_1', N'MILL_2', N'MILL_3', N'MILL_4',
        N'MILL_1_WIP', N'MILL_1_FG'))
    OR
    (Kind = N'Station' AND Printer_Key IN (
        N'VISUAL_REVISUAL', N'BIG_HYDRO', N'FOUR_HEAD_HYDRO'))
    OR
    (Kind = N'PasRoom' AND Printer_Key = N'PAS_ROOM')
);
GO

-- Seed Mill-1 WIP from existing MILL_1 when present; else plant default.
IF NOT EXISTS (SELECT 1 FROM dbo.Printer WHERE Printer_Key = N'MILL_1_WIP')
BEGIN
    INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_By)
    SELECT
        N'MILL_1_WIP',
        N'Mill',
        COALESCE(
            (SELECT TOP (1) Address FROM dbo.Printer WHERE Printer_Key = N'MILL_1'),
            N'192.168.0.54'),
        COALESCE(
            (SELECT TOP (1) Port FROM dbo.Printer WHERE Printer_Key = N'MILL_1'),
            9100),
        N'Seed';
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Printer WHERE Printer_Key = N'MILL_1_FG')
BEGIN
    INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_By)
    VALUES (N'MILL_1_FG', N'Mill', N'192.168.0.55', 9100, N'Seed');
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Printer WHERE Printer_Key = N'PAS_ROOM')
BEGIN
    INSERT INTO dbo.Printer (Printer_Key, Kind, Address, Port, Updated_By)
    VALUES (N'PAS_ROOM', N'PasRoom', N'192.168.0.124', 9100, N'Seed');
END
GO
