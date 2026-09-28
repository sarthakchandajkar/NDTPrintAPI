-- Additive: Input_Slit_Pending + Input_Slit_Pending_Row
-- Durable claim of parsed Input Slit rows so mill stamp/output does not depend on the
-- file still sitting in Z:\To SAP\TM\Input Slit (PPC may move it to Accepted).
-- Status: AwaitingTarget → Completed (NDT Batch stamped + Output CSV written from SQL).
-- Keyed by (Source_File_Name, Source_LastWriteTimeUtc, Mill_No).
-- Run against JazeeraMES_Prod (or Dev). Safe to re-run.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Input_Slit_Pending' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Input_Slit_Pending (
        Input_Slit_Pending_ID     BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Source_File_Name          NVARCHAR(260)  NOT NULL,
        Source_File_Claimed       NVARCHAR(512)  NULL,
        Source_LastWriteTimeUtc   DATETIME2(3)   NOT NULL,
        Mill_No                   INT            NOT NULL,
        Status                    NVARCHAR(32)   NOT NULL
            CONSTRAINT DF_Input_Slit_Pending_Status DEFAULT (N'AwaitingTarget'),
        NDT_Batch_No              NVARCHAR(20)   NULL,
        Output_File               NVARCHAR(512)  NULL,
        Claimed_AtUtc             DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Input_Slit_Pending_Claimed_AtUtc DEFAULT (SYSUTCDATETIME()),
        Completed_AtUtc           DATETIME2(3)   NULL,
        CONSTRAINT CK_Input_Slit_Pending_Status
            CHECK (Status IN (N'AwaitingTarget', N'Completed'))
    );

    CREATE UNIQUE INDEX UX_Input_Slit_Pending_File_Write_Mill
        ON dbo.Input_Slit_Pending (Source_File_Name, Source_LastWriteTimeUtc, Mill_No);

    CREATE INDEX IX_Input_Slit_Pending_Status_Mill
        ON dbo.Input_Slit_Pending (Status, Mill_No);
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Input_Slit_Pending_Row' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Input_Slit_Pending_Row (
        Input_Slit_Pending_Row_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Input_Slit_Pending_ID     BIGINT         NOT NULL,
        Source_Row_Number         INT            NOT NULL,
        PO_Number                 NVARCHAR(30)   NOT NULL,
        Slit_No                   NVARCHAR(50)   NULL,
        NDT_Pipes                 INT            NOT NULL
            CONSTRAINT DF_Input_Slit_Pending_Row_NDT_Pipes DEFAULT (0),
        Rejected_P                INT            NOT NULL
            CONSTRAINT DF_Input_Slit_Pending_Row_Rejected_P DEFAULT (0),
        Slit_Start_Time           DATETIME2(2)   NULL,
        Slit_Finish_Time          DATETIME2(2)   NULL,
        Mill_No                   INT            NULL,
        NDT_Short_Length_Pipe     NVARCHAR(50)   NULL,
        Rejected_Short_Length_Pipe NVARCHAR(50)  NULL,
        Pipe_Size                 NVARCHAR(50)   NULL,
        CONSTRAINT FK_Input_Slit_Pending_Row_Pending
            FOREIGN KEY (Input_Slit_Pending_ID)
            REFERENCES dbo.Input_Slit_Pending (Input_Slit_Pending_ID)
            ON DELETE CASCADE
    );

    CREATE UNIQUE INDEX UX_Input_Slit_Pending_Row_Pending_RowNo
        ON dbo.Input_Slit_Pending_Row (Input_Slit_Pending_ID, Source_Row_Number);
END
GO
