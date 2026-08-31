-- Additive: per-mill ZPL printer IP/port (operator-managed via Shared Settings).
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the mill-state-in-SQL binary.
-- Safe to re-run: table created only if missing; seed inserts only missing mill rows.
-- Seed matches today's office test printer (192.168.0.125:9100) on all four mills.

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Mill_Printer', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Mill_Printer
    (
        Mill_No        INT            NOT NULL
            CONSTRAINT PK_Mill_Printer PRIMARY KEY
            CONSTRAINT CK_Mill_Printer_Mill CHECK (Mill_No BETWEEN 1 AND 4),
        Address        NVARCHAR(128)  NOT NULL,
        Port           INT            NOT NULL
            CONSTRAINT CK_Mill_Printer_Port CHECK (Port BETWEEN 1 AND 65535),
        Updated_AtUtc  DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Mill_Printer_Updated DEFAULT (SYSUTCDATETIME()),
        Updated_By     NVARCHAR(128)  NULL
    );
END
GO

INSERT INTO dbo.Mill_Printer (Mill_No, Address, Port, Updated_By)
SELECT m.Mill_No, N'192.168.0.125', 9100, N'Seed'
FROM (VALUES (1), (2), (3), (4)) AS m(Mill_No)
WHERE NOT EXISTS (SELECT 1 FROM dbo.Mill_Printer p WHERE p.Mill_No = m.Mill_No);
GO
