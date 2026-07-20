-- Additive: Input_Slit_File_Seen — durable terminal marker for inbox files that must not be
-- re-queued by F-5 reconcile (e.g. no rows for configured mills).
-- Do NOT use sentinel rows in Input_Slit_Row (that table remains reconciliation ground truth).
-- Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Input_Slit_File_Seen' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Input_Slit_File_Seen (
        Input_Slit_File_Seen_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Source_File             NVARCHAR(512)  NOT NULL,
        Source_LastWriteTimeUtc DATETIME2(3)   NOT NULL,
        Reason                  NVARCHAR(64)   NOT NULL,
        Seen_AtUtc              DATETIME2(3)   NOT NULL CONSTRAINT DF_Input_Slit_File_Seen_Seen_AtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE UNIQUE INDEX UX_Input_Slit_File_Seen_File_Write
        ON dbo.Input_Slit_File_Seen (Source_File, Source_LastWriteTimeUtc);
END
GO
