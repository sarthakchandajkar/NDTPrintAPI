-- Data remediation for Mill-1 PO 1000060288 (2026-07-26 closed-PO late-slit orphan)
-- Run in JazeeraMES_Prod AFTER deploying Closed-PO traceability routing fix.
-- Review Section 0 (dispatch confirmation) before executing Section 2+.

SET NOCOUNT ON;

DECLARE @Po NVARCHAR(32) = N'1000060288';
DECLARE @Mill INT = 1;
DECLARE @GoodBundle NVARCHAR(20) = N'1226100001';
DECLARE @PhantomBundle NVARCHAR(20) = N'1226100002';
DECLARE @LateSlitFile NVARCHAR(128) = N'2604133_04_260726_1000060288.csv';
DECLARE @LockedTotal INT = 46; -- Manual_Recon corrected total (2026-07-26 ~15:01)

-- =============================================================================
-- 0) DISPATCH CONFIRMATION — run read-only; do not mutate until answered
-- =============================================================================
-- Incident: slit 2604133_04 (3 pcs) arrived ~33 min after PO end and was stamped on
-- phantom bundle 1226100002. At manual reconcile (14:59) bundle 1226100001 locked at
-- 46 pcs with CSV slitSum=49 (3 pcs above locked). Question for dispatch:
--
--   Were the 3 pipes on slit 2604133_04 already included in the physically-counted 46?
--     YES → Section 2A: reassign traceability only; Post_Recon may exceed 46 (expected drift).
--     NO  → Section 2B: same reassign + dispatch must decide if 46→49 reprint is required.
--
-- Evidence: slit 04 file timestamp 15:13 is AFTER manual recon; it was NOT in slitSum=49.

SELECT N'Bundle rows' AS Section,
       b.Bundle_No,
       b.PO_Number,
       b.Mill_No,
       b.Total_NDT_Pcs,
       b.Manual_Recon,
       b.Manual_Recon_Original_Total,
       b.Post_Recon_Csv_Sum,
       b.Print_Status,
       b.Manual_Review,
       b.Close_Source,
       b.Awaiting_Csv_Recon,
       b.PrintedAt
FROM dbo.NDT_Bundle b
WHERE b.PO_Number = @Po
  AND b.Mill_No = @Mill
  AND b.Bundle_No IN (@GoodBundle, @PhantomBundle)
ORDER BY b.Bundle_No;

SELECT N'Output slit rows by batch' AS Section,
       osr.NDT_Batch_No,
       osr.Source_File,
       osr.Slit_No,
       osr.NDT_Pipes,
       SUM(osr.NDT_Pipes) OVER (PARTITION BY osr.NDT_Batch_No) AS BatchSlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po
  AND osr.Mill_No = @Mill
  AND osr.NDT_Batch_No IN (@GoodBundle, @PhantomBundle)
ORDER BY osr.NDT_Batch_No, osr.Source_File, osr.Slit_No;

SELECT N'Late slit file detail' AS Section,
       isr.Source_File,
       isr.Slit_No,
       isr.NDT_Pipes,
       osr.NDT_Batch_No AS CurrentBatchAssignment
FROM dbo.Input_Slit_Row isr
LEFT JOIN dbo.Output_Slit_Row osr
  ON osr.Source_File = isr.Source_File
 AND osr.Source_Row_Number = isr.Source_Row_Number
WHERE isr.Source_File LIKE N'%' + @LateSlitFile;

SELECT N'Dispatch checklist' AS Section,
       @LockedTotal AS LockedPrintedTotal,
       (SELECT SUM(osr.NDT_Pipes)
        FROM dbo.Output_Slit_Row osr
        WHERE osr.NDT_Batch_No = @GoodBundle) AS GoodBundleCurrentSlitSum,
       (SELECT SUM(osr.NDT_Pipes)
        FROM dbo.Output_Slit_Row osr
        WHERE osr.NDT_Batch_No = @PhantomBundle) AS PhantomBundleSlitSum,
       CASE
           WHEN EXISTS (
               SELECT 1 FROM dbo.Output_Slit_Row osr
               WHERE osr.NDT_Batch_No = @GoodBundle
                 AND osr.Source_File LIKE N'%2604133_04_%'
           ) THEN N'2604133_04 already on good bundle'
           ELSE N'2604133_04 only on phantom — reassign required'
       END AS LateSlitPlacement;

-- =============================================================================
-- 1) Pre-check: phantom must not be printed / must belong to this PO
-- =============================================================================
IF NOT EXISTS (
    SELECT 1 FROM dbo.NDT_Bundle
    WHERE Bundle_No = @PhantomBundle AND PO_Number = @Po AND Mill_No = @Mill
)
BEGIN
    RAISERROR(N'Phantom bundle %s not found for PO %s Mill %d — abort.', 16, 1, @PhantomBundle, @Po, @Mill);
    RETURN;
END;

-- =============================================================================
-- 2A/2B) Reassign late slit traceability from phantom → locked bundle; void phantom
-- (Same SQL for both dispatch answers — reprint decision is operational, not schema.)
-- =============================================================================

BEGIN TRANSACTION;

-- Move Output_Slit_Row stamps off the phantom sequence
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @GoodBundle
WHERE NDT_Batch_No = @PhantomBundle
  AND PO_Number = @Po
  AND Mill_No = @Mill;

-- Void phantom bundle row (never printed; orphan from post-close ingest)
UPDATE dbo.NDT_Bundle
SET Manual_Review = 1,
    Print_Status = N'Void',
    Total_NDT_Pcs = 0,
    Awaiting_Csv_Recon = 0,
    Count_Discrepancy = 0
WHERE Bundle_No = @PhantomBundle
  AND PO_Number = @Po
  AND Mill_No = @Mill;

-- Recompute Post_Recon_Csv_Sum on the locked bundle from all attached output rows
DECLARE @RecomputedSlitSum INT;
SELECT @RecomputedSlitSum = ISNULL(SUM(osr.NDT_Pipes), 0)
FROM dbo.Output_Slit_Row osr
WHERE osr.NDT_Batch_No = @GoodBundle;

UPDATE dbo.NDT_Bundle
SET Post_Recon_Csv_Sum = @RecomputedSlitSum
WHERE Bundle_No = @GoodBundle
  AND Manual_Recon = 1;

COMMIT TRANSACTION;

-- =============================================================================
-- 3) Optional disk cleanup (UNC paths — run manually on file server if needed)
-- =============================================================================
-- Delete or archive: NDT_Bundle_1226100002.csv / .zpl under NDT Bundles folder if created.
-- Edit NDT Input Slit output 2604133_04_260726_1000060288.csv: change NDT Batch No column
--   1226100002 → 1226100001 (or re-ingest after deleting Output_Slit_Row + Input_Slit_Row
--   for that file so the deployed fix reprocesses it).

-- =============================================================================
-- 4) Post-remediation verification
-- =============================================================================
SELECT N'Post-remediation bundles' AS Section,
       b.Bundle_No,
       b.Total_NDT_Pcs AS LockedTotal,
       b.Post_Recon_Csv_Sum,
       b.Post_Recon_Csv_Sum - b.Total_NDT_Pcs AS CsvMinusLocked,
       b.Print_Status,
       b.Manual_Review,
       b.Manual_Recon
FROM dbo.NDT_Bundle b
WHERE b.PO_Number = @Po
  AND b.Mill_No = @Mill
  AND b.Bundle_No IN (@GoodBundle, @PhantomBundle);

SELECT N'Post-remediation slit sums' AS Section,
       osr.NDT_Batch_No,
       COUNT(*) AS RowCount,
       SUM(osr.NDT_Pipes) AS SlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po
  AND osr.Mill_No = @Mill
GROUP BY osr.NDT_Batch_No
ORDER BY osr.NDT_Batch_No;

-- Expected after fix:
--   1226100001: LockedTotal=46, Post_Recon_Csv_Sum=52 if 2604133_04 (3) adds to prior 49
--               (dispatch: 3 pcs likely already in physical 46 — CSV drift is traceability-only)
--   1226100002: Print_Status=Void, Total_NDT_Pcs=0, no Output_Slit_Row rows
