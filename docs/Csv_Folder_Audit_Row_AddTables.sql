-- =============================================================================
-- Full row-for-row Excel CSV audit replicas (every column, exact header names).
-- Complements lookup tables Slit_Accepted_Row / Fg_Bundle_Row used for upload.
--
-- Columns_Json is a JSON object: { "Exact Header": "cell", ... } for every CSV column.
-- Header_Line / Row_Line preserve the original Excel CSV text for forensic replay.
-- Run against JazeeraMES_Prod. Safe to re-run.
-- =============================================================================

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Slit_Accepted_Audit_Row' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Slit_Accepted_Audit_Row (
        Slit_Accepted_Audit_Row_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Source_File               NVARCHAR(500)  NOT NULL,
        Source_Row_Number         INT            NOT NULL,
        Header_Line               NVARCHAR(MAX)  NOT NULL,
        Row_Line                  NVARCHAR(MAX)  NOT NULL,
        Columns_Json              NVARCHAR(MAX)  NOT NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL
            CONSTRAINT DF_Slit_Accepted_Audit_Row_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Slit_Accepted_Audit_Source
        ON dbo.Slit_Accepted_Audit_Row (Source_File);

    CREATE INDEX IX_Slit_Accepted_Audit_Source_Row
        ON dbo.Slit_Accepted_Audit_Row (Source_File, Source_Row_Number);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Fg_Bundle_Audit_Row' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Fg_Bundle_Audit_Row (
        Fg_Bundle_Audit_Row_ID    BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Source_File               NVARCHAR(500)  NOT NULL,
        Source_Row_Number         INT            NOT NULL,
        Header_Line               NVARCHAR(MAX)  NOT NULL,
        Row_Line                  NVARCHAR(MAX)  NOT NULL,
        Columns_Json              NVARCHAR(MAX)  NOT NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL
            CONSTRAINT DF_Fg_Bundle_Audit_Row_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Fg_Bundle_Audit_Source
        ON dbo.Fg_Bundle_Audit_Row (Source_File);

    CREATE INDEX IX_Fg_Bundle_Audit_Source_Row
        ON dbo.Fg_Bundle_Audit_Row (Source_File, Source_Row_Number);
END
GO
