-- Additive: PLC mill PO lifecycle (Draining/Closed). Running = no row.
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the mill-state-in-SQL binary.
-- Safe to re-run: tables/indexes created only if missing. No JSON migration (fresh reset).

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Po_Lifecycle', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Po_Lifecycle
    (
        Mill_No              INT            NOT NULL
            CONSTRAINT CK_Po_Lifecycle_Mill CHECK (Mill_No BETWEEN 1 AND 4),
        Po_Number            NVARCHAR(32)   NOT NULL,
        Phase                NVARCHAR(16)   NOT NULL
            CONSTRAINT CK_Po_Lifecycle_Phase CHECK (Phase IN (N'Draining', N'Closed')),
        Ended_AtUtc          DATETIME2(3)   NOT NULL,
        Is_Resume_Candidate  BIT            NOT NULL
            CONSTRAINT DF_Po_Lifecycle_Resume DEFAULT (0),
        Updated_AtUtc        DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Po_Lifecycle_Updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Po_Lifecycle PRIMARY KEY (Mill_No, Po_Number)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Po_Lifecycle_Mill_Phase_Ended'
      AND object_id = OBJECT_ID(N'dbo.Po_Lifecycle'))
BEGIN
    CREATE INDEX IX_Po_Lifecycle_Mill_Phase_Ended
        ON dbo.Po_Lifecycle (Mill_No, Phase, Ended_AtUtc);
END
GO

IF OBJECT_ID(N'dbo.Po_Lifecycle_Audit', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Po_Lifecycle_Audit
    (
        Audit_ID             BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        Mill_No              INT            NOT NULL,
        Po_Number            NVARCHAR(32)   NOT NULL,
        Old_Phase            NVARCHAR(16)   NULL,
        New_Phase            NVARCHAR(16)   NOT NULL,
        Ended_AtUtc          DATETIME2(3)   NULL,
        Is_Resume_Candidate  BIT            NULL,
        Event_Type           NVARCHAR(32)   NOT NULL,
        AtUtc                DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Po_Lifecycle_Audit_AtUtc DEFAULT (SYSUTCDATETIME())
    );
    CREATE INDEX IX_Po_Lifecycle_Audit_Mill
        ON dbo.Po_Lifecycle_Audit (Mill_No, Po_Number, AtUtc);
END
GO
