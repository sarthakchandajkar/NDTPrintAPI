-- Quiet-drain deploy checklist for fill-to-target cutover.
-- Run BEFORE starting the new binary. Expect zero rows from query 1.

-- 1) No awaiting recon (mandatory)
SELECT Bundle_No, PO_Number, Mill_No, Total_NDT_Pcs, Close_Source, Print_Status
FROM dbo.NDT_Bundle
WHERE Awaiting_Csv_Recon = 1;
-- expect 0 rows

-- 2) Optional: review open Manual_Review / Count_Discrepancy on Mill-1
SELECT Bundle_No, PO_Number, Manual_Review, Count_Discrepancy, Total_NDT_Pcs
FROM dbo.NDT_Bundle
WHERE Mill_No = 1
  AND (Manual_Review = 1 OR Count_Discrepancy = 1)
ORDER BY PrintedAt DESC;

-- 3) Apply schema BEFORE binary start:
--    docs/NDT_Bundle_Alter_CsvFill.sql

-- 4) Runtime: NdtBundleRuntimeState.json — no live slot with ProvisionalBatchNo > 0,
--    RunningTotal > 0, or non-empty sizeCounts for an active PO. Prefer gap between POs.

-- 5) Start service with RequireCleanFillCutover=true (Production default).
--    Do not disable the guard to force a mistimed deploy.
