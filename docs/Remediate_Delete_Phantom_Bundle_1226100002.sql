-- Remove phantom NDT_Bundle 1226100002 (Mill-1) when Output_Slit_Row still references it.
-- Error without this order: FK_Output_Slit_Row_Bundle on NDT_Batch_No → NDT_Bundle.Bundle_No
--
-- NOTE: Bundle_No is 1226100002 (mill 1), not 1226200002 (mill 2).
-- Run Section 0 read-only first; execute Section 1 only after rows look correct.

SET NOCOUNT ON;

DECLARE @Po NVARCHAR(32) = N'1000060288';
DECLARE @Mill INT = 1;
DECLARE @GoodBundle NVARCHAR(20) = N'1226100001';
DECLARE @PhantomBundle NVARCHAR(20) = N'1226100002';

-- =============================================================================
-- 0) READ-ONLY — confirm phantom bundle and dependent slit rows
-- =============================================================================
SELECT N'NDT_Bundle' AS Section, b.*
FROM dbo.NDT_Bundle b
WHERE b.Bundle_No IN (@GoodBundle, @PhantomBundle)
ORDER BY b.Bundle_No;

SELECT N'Output_Slit_Row by batch' AS Section,
       osr.NDT_Batch_No,
       COUNT(*) AS SlitRowCount,
       SUM(osr.NDT_Pipes) AS SlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po
  AND osr.Mill_No = @Mill
  AND osr.NDT_Batch_No IN (@GoodBundle, @PhantomBundle)
GROUP BY osr.NDT_Batch_No
ORDER BY osr.NDT_Batch_No;

SELECT N'Output_Slit_Row detail (phantom)' AS Section,
       osr.Output_Slit_Row_ID,
       osr.NDT_Batch_No,
       osr.Source_File,
       osr.Slit_No,
       osr.NDT_Pipes
FROM dbo.Output_Slit_Row osr
WHERE osr.NDT_Batch_No = @PhantomBundle
ORDER BY osr.Source_File, osr.Slit_No;

-- =============================================================================
-- 1) FIX — reassign slit FK parent, then delete phantom bundle row
-- =============================================================================
BEGIN TRANSACTION;

UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @GoodBundle
WHERE NDT_Batch_No = @PhantomBundle
  AND PO_Number = @Po
  AND Mill_No = @Mill;

IF @@ROWCOUNT = 0
BEGIN
    ROLLBACK TRANSACTION;
    RAISERROR(N'No Output_Slit_Row rows updated for phantom %s — verify batch numbers before delete.', 16, 1, @PhantomBundle);
    RETURN;
END;

-- Recompute Post_Recon_Csv_Sum on the manually reconciled good bundle (if column exists).
IF COL_LENGTH(N'dbo.NDT_Bundle', N'Post_Recon_Csv_Sum') IS NOT NULL
BEGIN
    DECLARE @RecomputedSlitSum INT;
    SELECT @RecomputedSlitSum = ISNULL(SUM(osr.NDT_Pipes), 0)
    FROM dbo.Output_Slit_Row osr
    WHERE osr.NDT_Batch_No = @GoodBundle;

    UPDATE dbo.NDT_Bundle
    SET Post_Recon_Csv_Sum = @RecomputedSlitSum
    WHERE Bundle_No = @GoodBundle
      AND Manual_Recon = 1;
END;

DELETE FROM dbo.NDT_Bundle
WHERE Bundle_No = @PhantomBundle
  AND PO_Number = @Po
  AND Mill_No = @Mill;

IF @@ROWCOUNT <> 1
BEGIN
    ROLLBACK TRANSACTION;
    RAISERROR(N'Expected to delete exactly one NDT_Bundle row for %s.', 16, 1, @PhantomBundle);
    RETURN;
END;

COMMIT TRANSACTION;

-- =============================================================================
-- 2) VERIFY
-- =============================================================================
SELECT N'After fix — bundles' AS Section,
       b.Bundle_No,
       b.Total_NDT_Pcs,
       b.Manual_Recon,
       b.Post_Recon_Csv_Sum,
       b.Print_Status
FROM dbo.NDT_Bundle b
WHERE b.PO_Number = @Po
  AND b.Mill_No = @Mill
  AND b.Bundle_No IN (@GoodBundle, @PhantomBundle);

SELECT N'After fix — slit sums' AS Section,
       osr.NDT_Batch_No,
       COUNT(*) AS SlitRowCount,
       SUM(osr.NDT_Pipes) AS SlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po
  AND osr.Mill_No = @Mill
GROUP BY osr.NDT_Batch_No
ORDER BY osr.NDT_Batch_No;

-- Optional disk cleanup (manual on file share):
--   Delete or archive NDT_Bundle_1226100002.csv / .zpl under NDT Bundles folder.
--   Ensure NDT Input Slit output CSVs use NDT Batch No = 1226100001 (not 1226100002).
