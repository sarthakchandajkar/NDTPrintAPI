-- Data remediation for Mill-1 bundles 1226100002 / 1226100003 (2026-07-26 incident, POs 1000060288 + 1000060520)
-- Run in JazeeraMES_Prod AFTER deploying the stamp-corrector / dedupe / lifecycle-persistence fixes.
--
-- Incident chain being remediated:
--   1. 16:40  Bundle 1226100002 (PO 1000060288, 17 pcs) closed+printed from the drain window.
--   2. ~17:05 Remediate_Delete_Phantom_Bundle_1226100002.sql moved PO 288's slit rows to 1226100001
--             and DELETED the NDT_Bundle row 1226100002.
--   3. 17:2x  PO 1000060520's forming bundle still held provisional stamp 1226100002; its first new
--             slit row re-created NDT_Bundle 1226100002 (PO=1000060520, Total=0, PrintedAt=NULL).
--   4. 18:27  PO 520's close allocated final 1226100003; the SQL renumber 002→003 failed on
--             FK_Output_Slit_Row_Bundle and was swallowed — all PO-520 rows stayed on 002.
--             Result: bundle 1226100003 printed 87 pcs but has ZERO Output_Slit_Row rows.
--   5. 18:49  Operator manually reconciled 002 (original=39 → corrected=76) and reprinted its tag.
--   6. 20:53 / 21:23  Late re-drops re-attached files to 002 WITHOUT dedupe:
--             2604140_01 ×3 and 2604140_03 ×3 → Post_Recon_Csv_Sum inflated to 108 (unique sum is 76).
--
-- End state after this script:
--   * All PO-520 slit rows deduped and moved to 1226100003 (matches its printed tag).
--   * Bundle 1226100002 voided (kept as an FK/resurrection guard, PO restored to 1000060288).
--   * OPERATOR ACTION: physically retrieve and destroy the reprinted "1226100002 / 76 pcs" tag.
--     PO 288's pieces live under 1226100001; PO 520's pieces live under 1226100003.
--
-- Run Section 0 read-only first; execute Section 1 only after the rows look as described.

SET NOCOUNT ON;

DECLARE @Po520 NVARCHAR(32) = N'1000060520';
DECLARE @Po288 NVARCHAR(32) = N'1000060288';
DECLARE @Mill INT = 1;
DECLARE @Bundle002 NVARCHAR(20) = N'1226100002';
DECLARE @Bundle003 NVARCHAR(20) = N'1226100003';

-- =============================================================================
-- 0) READ-ONLY — confirm current damage
-- =============================================================================
SELECT N'NDT_Bundle' AS Section, b.*
FROM dbo.NDT_Bundle b
WHERE b.Bundle_No IN (@Bundle002, @Bundle003)
ORDER BY b.Bundle_No;

-- Expect: duplicated Source_File rows (2604140_01 ×3, 2604140_03 ×3) all on @Bundle002.
SELECT N'Output_Slit_Row (PO 520)' AS Section,
       osr.Output_Slit_Row_ID,
       osr.NDT_Batch_No,
       osr.Source_File,
       osr.Slit_No,
       osr.NDT_Pipes
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po520
ORDER BY osr.Source_File, osr.Output_Slit_Row_ID;

SELECT N'Slit sums by batch (PO 520)' AS Section,
       osr.NDT_Batch_No,
       COUNT(*) AS SlitRowCount,
       SUM(osr.NDT_Pipes) AS SlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po520
GROUP BY osr.NDT_Batch_No;

-- =============================================================================
-- 1) FIX — dedupe, re-home PO 520's rows to 1226100003, void 1226100002
-- =============================================================================
BEGIN TRANSACTION;

-- 1a) Dedupe: keep the earliest row per (Source_File, Slit_No, Source_Row_Number) for PO 520.
WITH Ranked AS (
    SELECT osr.Output_Slit_Row_ID,
           ROW_NUMBER() OVER (
               PARTITION BY osr.Source_File, osr.Slit_No, osr.Source_Row_Number
               ORDER BY osr.Output_Slit_Row_ID
           ) AS rn
    FROM dbo.Output_Slit_Row osr
    WHERE osr.PO_Number = @Po520
      AND osr.Mill_No = @Mill
)
DELETE FROM Ranked WHERE rn > 1;

PRINT CONCAT(N'Duplicate Output_Slit_Row rows removed: ', @@ROWCOUNT);

-- 1b) Guard: 1226100003 must already exist (printed 87 pcs at 18:27).
IF NOT EXISTS (SELECT 1 FROM dbo.NDT_Bundle WHERE Bundle_No = @Bundle003)
BEGIN
    ROLLBACK TRANSACTION;
    RAISERROR(N'NDT_Bundle row %s not found — verify before re-homing slit rows.', 16, 1, @Bundle003);
    RETURN;
END;

-- 1c) Move ALL PO-520 slit rows to the bundle whose tag was actually printed (1226100003).
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @Bundle003
WHERE PO_Number = @Po520
  AND Mill_No = @Mill
  AND NDT_Batch_No = @Bundle002;

IF @@ROWCOUNT = 0
BEGIN
    ROLLBACK TRANSACTION;
    RAISERROR(N'No Output_Slit_Row rows moved from %s to %s — verify batch numbers.', 16, 1, @Bundle002, @Bundle003);
    RETURN;
END;

-- 1d) Void 1226100002 but KEEP the row: deleting bundle rows is what allowed the silent
--     resurrection under the wrong PO (see 2026-07-26 17:05 remediation). Restore PO 288
--     ownership, zero the totals, clear the manual-recon lock, flag for review.
UPDATE dbo.NDT_Bundle
SET PO_Number = @Po288,
    Total_NDT_Pcs = 0,
    Print_Status = N'Void',
    Manual_Review = 1,
    Awaiting_Csv_Recon = 0,
    Count_Discrepancy = 0,
    Manual_Recon = 0,
    Manual_Recon_By = NULL,
    Manual_Recon_At = NULL,
    Manual_Recon_Reason = N'Voided 2026-07-27 remediation: number collided between PO 1000060288 (17 pcs, folded into 1226100001) and PO 1000060520 (rows moved to 1226100003). Destroy the reprinted 76-pc tag.',
    Manual_Recon_Original_Total = NULL,
    Post_Recon_Csv_Sum = NULL
WHERE Bundle_No = @Bundle002;

-- 1e) Flag 1226100003 for review: printed tag says 87 pcs; file-side slit sum is expected
--     to be 76 after the move (late/missing files) — operator must confirm the physical count.
UPDATE dbo.NDT_Bundle
SET Manual_Review = 1,
    Count_Discrepancy = CASE
        WHEN (SELECT ISNULL(SUM(osr.NDT_Pipes), 0)
              FROM dbo.Output_Slit_Row osr
              WHERE osr.NDT_Batch_No = @Bundle003) <> Total_NDT_Pcs THEN 1
        ELSE Count_Discrepancy
    END
WHERE Bundle_No = @Bundle003;

COMMIT TRANSACTION;

-- =============================================================================
-- 2) VERIFY
-- =============================================================================
SELECT N'After fix — bundles' AS Section,
       b.Bundle_No,
       b.PO_Number,
       b.Total_NDT_Pcs,
       b.Print_Status,
       b.Manual_Review,
       b.Manual_Recon,
       b.Post_Recon_Csv_Sum
FROM dbo.NDT_Bundle b
WHERE b.Bundle_No IN (@Bundle002, @Bundle003);

-- Expect: 1226100002 → 0 rows; 1226100003 → 14 rows / 76 pcs (unique files).
SELECT N'After fix — slit sums' AS Section,
       osr.NDT_Batch_No,
       COUNT(*) AS SlitRowCount,
       SUM(osr.NDT_Pipes) AS SlitSum
FROM dbo.Output_Slit_Row osr
WHERE osr.PO_Number = @Po520
GROUP BY osr.NDT_Batch_No;

-- Manual/physical follow-ups:
--   * Destroy the reprinted tag "1226100002 / 76 pcs" (18:49 reprint); tag 1226100003 (87 pcs) stands.
--   * Archive NDT_Bundle_1226100002.csv / .zpl on the file share if still present.
--   * Per-slit output CSVs for PO 520 were already rewritten to 1226100003 on disk at 18:27 —
--     after this script SQL and disk agree again.
