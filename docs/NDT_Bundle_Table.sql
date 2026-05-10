-- Run this script in SQL Server Management Studio to create the NDT_Bundle table
-- used for reconciliation (Reconcile Bundle feature).
-- Database: create or use existing, e.g. CREATE DATABASE JazeeraMES_Dev; USE JazeeraMES_Dev; (or JazeeraMES_Prod)

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'NDT_Bundle')
BEGIN
    CREATE TABLE dbo.NDT_Bundle (
        NDTBundle_ID    BIGINT         IDENTITY(1,1) NOT NULL PRIMARY KEY,
        PO_Number       NVARCHAR(30)   NOT NULL,
        Mill_No         INT            NOT NULL,
        Bundle_No       NVARCHAR(20)   NOT NULL,
        Total_NDT_Pcs   INT            NOT NULL,
        Context_Slit_No NVARCHAR(50)   NULL,
        Slit_Start_Time DATETIME2(2)   NULL,
        Slit_Finish_Time DATETIME2(2)  NULL,
        Rejected_P      INT            NOT NULL DEFAULT 0,
        NDT_Short_Length_Pipe NVARCHAR(50) NULL,
        Rejected_Short_Length_Pipe NVARCHAR(50) NULL,
        PrintedAt       DATETIME2(2)   NOT NULL DEFAULT SYSDATETIME(),
        IsReprint       BIT            NOT NULL DEFAULT 0
    );

    CREATE UNIQUE INDEX UQ_NDT_Bundle_Bundle_No ON dbo.NDT_Bundle (Bundle_No);
    CREATE INDEX IX_NDT_Bundle_PO_Mill ON dbo.NDT_Bundle (PO_Number, Mill_No);
END
GO
