-- =============================================================================
-- dbo.Fg_Bundle_Row — mirror of TM FG_*.csv rows from Bundle / Bundle Accepted
-- Example source (Z:\To SAP\TM\Bundle\FG_02_1000062356_2602021514_260930_135714.csv):
--
-- PO Number,Mill Line No,Pipe Grade,Pipe Size,Pipe Thickness,Pipe Length,Pipe Wt/mtr,
-- Pipe Type,Act Pcs. in Bundle,Bundle No,Bundle Status,Slit 1 Num,Slit 1 OK Pcs,
-- Slit 2 Num,Slit 2 OK Pcs,Slit 3 Num,Slit 3 OK Pcs,Slit 4 Num,Slit 4 OK Pcs,
-- Bundle Wt.,Bundle Start,Bundle End,Operator Done,Non Standard Slit,PO_Specification
--
-- One SQL row = one data row from one FG file (same columns as the Excel CSV).
-- Run against JazeeraMES_Prod. Safe to re-run.
-- =============================================================================

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Fg_Bundle_Row' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Fg_Bundle_Row (
        Fg_Bundle_Row_ID          BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Mill_No                   INT            NULL,
        Pipe_Grade                NVARCHAR(100)  NULL,
        Pipe_Size                 NVARCHAR(50)   NULL,
        Pipe_Thickness            NVARCHAR(50)   NULL,
        Pipe_Length               NVARCHAR(50)   NULL,
        Pipe_Weight_Per_Meter     NVARCHAR(50)   NULL,
        Pipe_Type                 NVARCHAR(50)   NULL,
        Act_Pcs_In_Bundle         NVARCHAR(50)   NULL,
        Bundle_No                 NVARCHAR(50)   NULL,
        Bundle_Status             NVARCHAR(50)   NULL,
        Slit_1_Num                NVARCHAR(50)   NULL,
        Slit_1_Ok_Pcs             NVARCHAR(50)   NULL,
        Slit_2_Num                NVARCHAR(50)   NULL,
        Slit_2_Ok_Pcs             NVARCHAR(50)   NULL,
        Slit_3_Num                NVARCHAR(50)   NULL,
        Slit_3_Ok_Pcs             NVARCHAR(50)   NULL,
        Slit_4_Num                NVARCHAR(50)   NULL,
        Slit_4_Ok_Pcs             NVARCHAR(50)   NULL,
        Bundle_Wt                 NVARCHAR(50)   NULL,
        Bundle_Start              NVARCHAR(50)   NULL,
        Bundle_End                NVARCHAR(50)   NULL,
        Operator_Done             NVARCHAR(50)   NULL,
        Non_Standard_Slit         NVARCHAR(100)  NULL,
        PO_Specification          NVARCHAR(500)  NULL,
        Source_File               NVARCHAR(500)  NULL,
        ImportedAtUtc             DATETIME2(2)   NOT NULL
            CONSTRAINT DF_Fg_Bundle_Row_ImportedAtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE INDEX IX_Fg_Bundle_Row_PO_Mill
        ON dbo.Fg_Bundle_Row (PO_Number, Mill_No);

    CREATE INDEX IX_Fg_Bundle_Row_Bundle
        ON dbo.Fg_Bundle_Row (Bundle_No);

    CREATE INDEX IX_Fg_Bundle_Row_Source
        ON dbo.Fg_Bundle_Row (Source_File);
END
GO
