-- Additive: per-mill live PLC snapshot for Shared dashboard tiles only.
-- Mill-n MERGEs its in-memory handshake snapshot ~500ms on a background loop.
-- PO-end, threshold/hooter, slit ingest, and tag print stay in the mill process
-- (this table is not on that path). Shared :5000 reads all four rows for plc-live.
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the mill-status binary.
-- Safe to re-run: table created only if missing.

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Mill_Instance_Status', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Mill_Instance_Status
    (
        Mill_No                  INT              NOT NULL
            CONSTRAINT PK_Mill_Instance_Status PRIMARY KEY
            CONSTRAINT CK_Mill_Instance_Status_Mill CHECK (Mill_No BETWEEN 1 AND 4),
        Instance_Id              UNIQUEIDENTIFIER NOT NULL,
        Machine_Name             NVARCHAR(128)    NOT NULL,
        Service_Name             NVARCHAR(128)    NULL,
        Mill_Name                NVARCHAR(64)     NOT NULL,
        Ip_Address               NVARCHAR(64)     NULL,
        Connected                BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Connected DEFAULT (0),
        Plc_Connection_Enabled   BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_PlcEn DEFAULT (1),
        Trigger_Active           BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Trig DEFAULT (0),
        Ack_Active               BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Ack DEFAULT (0),
        Handshake_State          NVARCHAR(64)     NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_State DEFAULT (N'Idle'),
        Last_Error               NVARCHAR(400)    NULL,
        Last_Update_AtUtc        DATETIME2(3)     NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Updated DEFAULT (SYSUTCDATETIME()),
        Ok_Count                 INT              NULL,
        Nok_Count                INT              NULL,
        Ndt_Count                INT              NULL,
        Po_Id                    INT              NULL,
        Slit_Id                  INT              NULL,
        Counts_Updated_AtUtc     DATETIME2(3)     NULL,
        Line_Running             BIT              NULL,
        Accumulated_Value        INT              NULL,
        Threshold_Value          INT              NULL,
        Hooter_Active            BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Hooter DEFAULT (0),
        Stuck_Trigger_Alarm      BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_Stuck DEFAULT (0),
        Ack_Write_Failed_Alarm   BIT              NOT NULL
            CONSTRAINT DF_Mill_Instance_Status_AckFail DEFAULT (0),
        Last_Po_End_Po_Id        INT              NULL,
        Last_Po_End_Ndt          INT              NULL,
        Last_Po_End_AtUtc        DATETIME2(3)     NULL
    );
END
GO
