-- Additive: Handshake_Event audit table for Phase 3 F-6.2.
-- Run against JazeeraMES_Prod (or Dev) before deploying NdtBundleService Phase 3 F-6.
-- Safe to re-run: table created only if missing.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Handshake_Event' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.Handshake_Event (
        Handshake_Event_ID BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Mill_No            INT            NOT NULL,
        Edge_AtUtc         DATETIME2(3)   NOT NULL,
        Ack_AtUtc          DATETIME2(3)   NULL,
        Cleared_AtUtc      DATETIME2(3)   NULL,
        Ack_Dropped_AtUtc  DATETIME2(3)   NULL,
        Plc_Po_Id          INT            NOT NULL CONSTRAINT DF_Handshake_Event_Plc_Po_Id DEFAULT (0),
        Plc_Ndt_Count      INT            NOT NULL CONSTRAINT DF_Handshake_Event_Plc_Ndt_Count DEFAULT (0),
        Correlation_Id     UNIQUEIDENTIFIER NOT NULL,
        Outcome            NVARCHAR(64)   NOT NULL,
        Error_Message      NVARCHAR(500)  NULL,
        Updated_AtUtc      DATETIME2(3)   NOT NULL CONSTRAINT DF_Handshake_Event_Updated_AtUtc DEFAULT (SYSUTCDATETIME())
    );

    CREATE UNIQUE INDEX UX_Handshake_Event_Correlation ON dbo.Handshake_Event (Correlation_Id);
    CREATE INDEX IX_Handshake_Event_Mill_Edge ON dbo.Handshake_Event (Mill_No, Edge_AtUtc DESC);
END
GO
