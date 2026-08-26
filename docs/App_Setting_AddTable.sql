-- Additive: global App_Setting key/value store (ZPL print toggle, etc.).
-- Run against JazeeraMES_Prod (or Dev) before deploying split NdtBundleService instances.
-- Safe to re-run: table and seed row created only if missing.

USE JazeeraMES_Prod;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'App_Setting' AND schema_id = SCHEMA_ID(N'dbo'))
BEGIN
    CREATE TABLE dbo.App_Setting (
        Setting_Key   NVARCHAR(64)  NOT NULL PRIMARY KEY,
        Setting_Value NVARCHAR(256) NOT NULL,
        Updated_AtUtc DATETIME2(3)  NOT NULL CONSTRAINT DF_App_Setting_Updated_AtUtc DEFAULT (SYSUTCDATETIME()),
        Updated_By    NVARCHAR(128) NULL
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.App_Setting WHERE Setting_Key = N'ZplPhysicalPrintEnabled')
BEGIN
    INSERT INTO dbo.App_Setting (Setting_Key, Setting_Value, Updated_By)
    VALUES (N'ZplPhysicalPrintEnabled', N'true', N'system');
END
GO
