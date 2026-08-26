-- Additive: per-mill exclusive lease so two processes cannot run mill workers for the same mill.
-- Run against JazeeraMES_Prod (or Dev) BEFORE starting split Mill / Monolith instances.
-- Safe to re-run.

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Mill_Instance_Lease', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Mill_Instance_Lease
    (
        Mill_No              INT            NOT NULL
            CONSTRAINT PK_Mill_Instance_Lease PRIMARY KEY
            CONSTRAINT CK_Mill_Instance_Lease_Mill_No CHECK (Mill_No BETWEEN 1 AND 4),
        Instance_Id          UNIQUEIDENTIFIER NOT NULL,
        Machine_Name         NVARCHAR(128)  NOT NULL,
        Service_Name         NVARCHAR(128)  NULL,
        Process_Start_AtUtc  DATETIME2(3)   NOT NULL,
        Lease_Acquired_AtUtc DATETIME2(3)   NOT NULL,
        Lease_Renewed_AtUtc  DATETIME2(3)   NOT NULL,
        Lease_Expires_AtUtc  DATETIME2(3)   NOT NULL
    );
END
GO

-- Optional seed: empty table; rows appear on first successful claim.
