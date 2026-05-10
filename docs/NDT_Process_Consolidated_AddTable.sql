-- Add NDT_Process_Consolidated to an existing Jazeera MES database (SQL Server), e.g. JazeeraMES_Dev or JazeeraMES_Prod.
-- Safe to run once; skips if the table already exists.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

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
