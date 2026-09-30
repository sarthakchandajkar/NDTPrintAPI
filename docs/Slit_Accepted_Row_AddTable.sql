-- =============================================================================
-- dbo.Slit_Accepted_Row — mirror of Slitting "Slit Accepted" CSVs
-- (Z:\To SAP\TM\Slitting\Slit Accepted) for upload Slit Width lookups.
--
-- Sample folder was not reachable from the build host at authoring time.
-- Columns match what NdtBundleService.SlitAcceptedCsvLookup already reads
-- (Slit_No / Batch_No + Slit Width / Width, including paired …Batch_No / …Width
-- columns), plus optional columns when present on the CSV header.
-- One SQL row = one slit identity found in a source file (file may yield many rows).
-- Run against JazeeraMES_Prod. Safe to re-run.
-- =============================================================================

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Slit_Accepted_Row' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Slit_Accepted_Row (
        Slit_Accepted_Row_ID      BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Slit_No                   NVARCHAR(50)   NOT NULL,
        Slit_Width                NVARCHAR(50)   NULL,
        HRC_Number                NVARCHAR(50)   NULL,
        Slit_Thick                NVARCHAR(50)   NULL,
        PO_Number                 NVARCHAR(30)   NULL,
        Mill_No                   INT            NULL,
        NSS                       NVARCHAR(50)   NULL,
        Source_File               NVARCHAR(500)  NULL,
        Source_Row_Number         INT            NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL
            CONSTRAINT DF_Slit_Accepted_Row_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Slit_Accepted_Row_Slit
        ON dbo.Slit_Accepted_Row (Slit_No);

    CREATE INDEX IX_Slit_Accepted_Row_Source
        ON dbo.Slit_Accepted_Row (Source_File);
END
GO
