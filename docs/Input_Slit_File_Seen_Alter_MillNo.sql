-- Forward-only: scope Input_Slit_File_Seen by Mill_No so a non-owner Mill instance
-- MarkSeen(NoConfiguredMillRows) cannot poison the owning mill's reconcile.
-- Existing rows get Mill_No = 0 (legacy global markers); new writes use 1–4.

IF OBJECT_ID(N'dbo.Input_Slit_File_Seen', N'U') IS NOT NULL
   AND COL_LENGTH('dbo.Input_Slit_File_Seen', 'Mill_No') IS NULL
BEGIN
    ALTER TABLE dbo.Input_Slit_File_Seen
        ADD Mill_No INT NOT NULL
            CONSTRAINT DF_Input_Slit_File_Seen_Mill_No DEFAULT (0);
END
GO

IF OBJECT_ID(N'dbo.Input_Slit_File_Seen', N'U') IS NOT NULL
   AND EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UX_Input_Slit_File_Seen_File_Write'
      AND object_id = OBJECT_ID(N'dbo.Input_Slit_File_Seen'))
BEGIN
    DROP INDEX UX_Input_Slit_File_Seen_File_Write ON dbo.Input_Slit_File_Seen;
END
GO

IF OBJECT_ID(N'dbo.Input_Slit_File_Seen', N'U') IS NOT NULL
   AND NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UX_Input_Slit_File_Seen_File_Write_Mill'
      AND object_id = OBJECT_ID(N'dbo.Input_Slit_File_Seen'))
BEGIN
    CREATE UNIQUE INDEX UX_Input_Slit_File_Seen_File_Write_Mill
        ON dbo.Input_Slit_File_Seen (Source_File, Source_LastWriteTimeUtc, Mill_No);
END
GO
