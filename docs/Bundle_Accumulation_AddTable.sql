-- Additive: open-bundle remainder per mill/PO/size, plus last-slit print context.
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the mill-state-in-SQL binary.
-- Safe to re-run: tables/indexes created only if missing. No JSON migration (fresh reset).

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Bundle_Accumulation', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Bundle_Accumulation
    (
        Mill_No            INT            NOT NULL
            CONSTRAINT CK_Bundle_Accumulation_Mill CHECK (Mill_No BETWEEN 1 AND 4),
        Po_Number          NVARCHAR(32)   NOT NULL,
        Size_Key           NVARCHAR(64)   NOT NULL,
        Pcs                INT            NOT NULL
            CONSTRAINT CK_Bundle_Accumulation_Pcs CHECK (Pcs > 0),
        Last_Activity_Utc  DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Bundle_Accumulation_Activity DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Bundle_Accumulation PRIMARY KEY (Mill_No, Po_Number, Size_Key)
    );
END
GO

-- Sweep orphan quiescence: per-mill MAX(Last_Activity_Utc) for a Closed PO.
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Bundle_Accumulation_Mill_Po_Activity'
      AND object_id = OBJECT_ID(N'dbo.Bundle_Accumulation'))
BEGIN
    CREATE INDEX IX_Bundle_Accumulation_Mill_Po_Activity
        ON dbo.Bundle_Accumulation (Mill_No, Po_Number, Last_Activity_Utc);
END
GO

-- RequireCleanFillCutover EXISTS (Mill_No = @owned): PK already leads with Mill_No (seek).
-- Covering index for mill-wide open-state scan (hooter / cutover).
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_Bundle_Accumulation_Mill'
      AND object_id = OBJECT_ID(N'dbo.Bundle_Accumulation'))
BEGIN
    CREATE INDEX IX_Bundle_Accumulation_Mill
        ON dbo.Bundle_Accumulation (Mill_No)
        INCLUDE (Po_Number, Size_Key, Pcs);
END
GO

-- Last slit used as CSV/ZPL close context. Not reconstructed from Input_Slit_Row
-- (importedAtUtc newest can diverge from the last slit the mill process held).
IF OBJECT_ID(N'dbo.Bundle_Accumulation_Context', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Bundle_Accumulation_Context
    (
        Mill_No                       INT            NOT NULL
            CONSTRAINT CK_Bundle_Acc_Ctx_Mill CHECK (Mill_No BETWEEN 1 AND 4),
        Po_Number                     NVARCHAR(32)   NOT NULL,
        Slit_No                       NVARCHAR(50)   NULL,
        Rejected_Pipes                INT            NOT NULL
            CONSTRAINT DF_Bundle_Acc_Ctx_Rejected DEFAULT (0),
        Slit_Start_Time               DATETIME2(3)   NULL,
        Slit_Finish_Time              DATETIME2(3)   NULL,
        Ndt_Short_Length_Pipe         NVARCHAR(50)   NULL,
        Rejected_Short_Length_Pipe    NVARCHAR(50)   NULL,
        Last_Activity_Utc             DATETIME2(3)   NOT NULL
            CONSTRAINT DF_Bundle_Acc_Ctx_Activity DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_Bundle_Accumulation_Context PRIMARY KEY (Mill_No, Po_Number)
    );
END
GO
