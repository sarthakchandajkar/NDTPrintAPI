-- Additive: per-mill NDT batch sequence authority (last allocated 5-digit sequence).
-- Run against JazeeraMES_Prod (or Dev) BEFORE deploying the mill-sequence binary.
-- Safe to re-run: table created only if missing; seed inserts only missing mill rows.
-- Seed is MAX of live (non-voided) current-year Bundle_No sequences; app first-start
-- may raise a missing row from InitialMillBatchNumbers / leftover JSON millMaxSequence.

USE JazeeraMES_Prod;
GO

IF OBJECT_ID(N'dbo.Mill_Sequence', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Mill_Sequence
    (
        Mill_No            INT            NOT NULL
            CONSTRAINT PK_Mill_Sequence PRIMARY KEY
            CONSTRAINT CK_Mill_Sequence_Mill_No CHECK (Mill_No BETWEEN 1 AND 4),
        Current_Sequence    INT            NOT NULL
            CONSTRAINT CK_Mill_Sequence_Current CHECK (Current_Sequence >= 0),
        Updated_AtUtc       DATETIME2(3)  NOT NULL
            CONSTRAINT DF_Mill_Sequence_Updated DEFAULT (SYSUTCDATETIME()),
        Updated_By         NVARCHAR(128)  NOT NULL
            CONSTRAINT DF_Mill_Sequence_UpdatedBy DEFAULT (N'Migration'),
        Reason              NVARCHAR(512)  NOT NULL
            CONSTRAINT DF_Mill_Sequence_Reason DEFAULT (N'Seed from NDT_Bundle')
    );
END
GO

IF OBJECT_ID(N'dbo.Mill_Sequence_Audit', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.Mill_Sequence_Audit
    (
        Audit_ID            BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
        Mill_No             INT            NOT NULL,
        Old_Sequence        INT            NULL,
        New_Sequence         INT            NOT NULL,
        Event_Type          NVARCHAR(32)  NOT NULL,
        Updated_By          NVARCHAR(128)  NOT NULL,
        Reason              NVARCHAR(512)  NOT NULL,
        AtUtc               DATETIME2(3)  NOT NULL
            CONSTRAINT DF_Mill_Sequence_Audit_AtUtc DEFAULT (SYSUTCDATETIME())
    );
    CREATE INDEX IX_Mill_Sequence_Audit_Mill
        ON dbo.Mill_Sequence_Audit (Mill_No, AtUtc);
END
GO

-- Seed missing mills from live NDT_Bundle (exclude tombstones / Voided=1 when column exists).
DECLARE @yy CHAR(2) = RIGHT(CONVERT(CHAR(4), YEAR(GETDATE())), 2);
DECLARE @prefix CHAR(2) = N'12';

;WITH Live AS (
    SELECT
        Mill_No,
        TRY_CONVERT(int, RIGHT(Bundle_No, 5)) AS Seq
    FROM dbo.NDT_Bundle
    WHERE LEN(Bundle_No) = 10
      AND LEFT(Bundle_No, 2) = @prefix
      AND SUBSTRING(Bundle_No, 3, 2) = @yy
      AND Total_NDT_Pcs > 0
)
INSERT INTO dbo.Mill_Sequence (Mill_No, Current_Sequence, Updated_By, Reason)
SELECT m.Mill_No, COALESCE(MAX(Live.Seq), 0), N'Migration', N'Seed from NDT_Bundle'
FROM (VALUES (1), (2), (3), (4)) AS m(Mill_No)
LEFT JOIN Live ON Live.Mill_No = m.Mill_No
WHERE NOT EXISTS (SELECT 1 FROM dbo.Mill_Sequence s WHERE s.Mill_No = m.Mill_No)
GROUP BY m.Mill_No;
GO
